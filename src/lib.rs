use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::channel::oneshot;
use futures::{SinkExt, Stream};
use nekoton::transport::models::ExistingContract;
use nekoton_utils::SimpleClock;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message};
use reqwest::StatusCode;
use serde::Serialize;
use ton_block::{Deserializable, MsgAddressInt};
use ton_block_compressor::ZstdWrapper;
use ton_types::UInt256;
use url::Url;

pub struct TransactionProducer {
    consumer_config: rdkafka::ClientConfig,
    states_url: Url,
    states_client: reqwest::Client,
    topic: String,
}

const NUM_TOPCIS: u8 = 9;

macro_rules! try_res {
    ($some:expr, $msg:literal) => {
        match $some {
            Ok(a) => a,
            Err(e) => {
                ::log::error!("{}:{:?}", $msg, e);
                continue;
            }
        }
    };
}

macro_rules! try_opt {
    ($some:expr, $msg:literal) => {
        match $some {
            Some(a) => a,
            None => {
                ::log::error!("{}", $msg);
                continue;
            }
        }
    };
}

pub struct ProducedTransaction {
    pub id: UInt256,
    pub transaction: ton_block::Transaction,
    commit_channel: Option<oneshot::Sender<()>>,
}

impl ProducedTransaction {
    fn new(id: UInt256, transaction: ton_block::Transaction) -> (Self, oneshot::Receiver<()>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                id,
                transaction,
                commit_channel: Some(tx),
            },
            rx,
        )
    }
    pub fn commit(&mut self) -> Result<()> {
        let committer = self.commit_channel.take().context("Already committed")?;
        committer
            .send(())
            .map_err(|_| anyhow::anyhow!("Failed committing"))?;
        Ok(())
    }

    pub fn get_inner_mut(&mut self) -> (UInt256, ton_block::Transaction) {
        (
            std::mem::take(&mut self.id),
            std::mem::take(&mut self.transaction),
        )
    }
}

#[derive(Serialize)]
struct StateReceiveRequest {
    account_id: String,
}

impl TransactionProducer {
    pub fn new<U>(
        group_id: &str,
        topic: String,
        states_rpc_endpoint: U,
        options: HashMap<&str, &str>,
    ) -> Result<Arc<Self>>
    where
        U: AsRef<str>,
    {
        let mut config = ClientConfig::default();
        config
            .set("group.id", group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "6000")
            .set("enable.partition.eof", "false");

        for (k, v) in options {
            config.set(k, v);
        }
        let states_rpc_endpoint =
            Url::parse(states_rpc_endpoint.as_ref()).context("Bad rpc endpoint")?;

        Ok(Arc::new(Self {
            consumer_config: config,
            states_url: states_rpc_endpoint.join("account").unwrap(),
            states_client: Default::default(),
            topic,
        }))
    }

    /// Cancel safety.
    /// Panics, if stream is dropped
    #[must_use]
    pub async fn stream_blocks(
        self: Arc<Self>,
        reset: bool,
        process_masterchain: bool,
    ) -> Result<impl Stream<Item = ProducedTransaction>> {
        let (tx, rx) = futures::channel::mpsc::channel(1);

        let offset = if reset {
            rdkafka::Offset::Beginning
        } else {
            rdkafka::Offset::Stored
        };
        let consumers = (0..NUM_TOPCIS)
            .filter(|x| if !process_masterchain { *x != 0 } else { true })
            .map(|partition| {
                let mut assignment = rdkafka::TopicPartitionList::new();
                assignment
                    .add_partition_offset(&self.topic, partition as i32, offset)
                    .unwrap();

                let consumer: StreamConsumer = self.consumer_config.create().unwrap_or_else(|e| {
                    panic!(
                        "Consumer creation failed for partition {} - {}",
                        partition, e
                    )
                });

                consumer.assign(&assignment).unwrap();
                consumer
            })
            .collect();
        tokio::spawn(async move { listen_consumer(consumers, tx).await });

        Ok(rx)
    }

    pub async fn get_contract_state(
        &self,
        contract_address: &MsgAddressInt,
    ) -> Result<Option<ExistingContract>> {
        let req = StateReceiveRequest {
            account_id: hex::encode(contract_address.address().get_bytestring_on_stack(0)),
        };
        let response = self
            .states_client
            .post(self.states_url.clone()) //todo improve?
            .json(&req)
            .send()
            .await
            .context("Failed sending request")?;

        if let StatusCode::OK = response.status() {
            Ok(Some(response.json().await?))
        } else {
            Ok(None)
        }
    }

    pub async fn run_local(
        &self,
        contract_address: &MsgAddressInt,
        function: &ton_abi::Function,
        input: &[ton_abi::Token],
    ) -> Result<Option<nekoton_abi::ExecutionOutput>> {
        use nekoton_abi::FunctionExt;

        let state = match self.get_contract_state(contract_address).await? {
            Some(a) => a,
            None => return Ok(None),
        };
        function
            .clone()
            .run_local(
                &SimpleClock,
                state.account,
                &state.last_transaction_id,
                input,
            )
            .map(|x| Some(x))
    }
}

async fn listen_consumer(
    consumers: Vec<StreamConsumer>,
    mut channel: futures::channel::mpsc::Sender<ProducedTransaction>,
) {
    let mut decompressor = ZstdWrapper::new();
    let mut cur = 0;
    let num_partitions = consumers.len();
    log::info!("Starting {} subscribers", num_partitions);

    loop {
        let prod = &consumers[cur as usize];
        let message = loop {
            match prod.recv().await {
                Ok(a) => break a,
                Err(e) => {
                    log::error!("Consumer {} is broken: {:?}", cur, e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            };
        };

        let payload = try_opt!(message.payload(), "no payload");
        let payload_decompressed = try_res!(
            decompressor.decompress(payload),
            "Failed decompressing block data"
        );
        let transaction = try_res!(
            ton_block::Transaction::construct_from_bytes(payload_decompressed),
            "Failed constructing block"
        );

        let key = try_opt!(message.key(), "No key");
        let key = UInt256::from_slice(key);

        let (block, rx) = ProducedTransaction::new(key, transaction);
        if let Err(_) = channel.send(block).await {
            panic!("Failed sending via channel");
        }

        if let Err(_) = rx.await {
            continue;
        } //waiting for commit

        if let Err(e) = prod.commit_consumer_state(CommitMode::Async) {
            log::error!("Failed committing: {:?}", e);
            continue;
        }

        cur = (cur + 1) % (num_partitions);
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::default::Default;
    use std::io::Write;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::time::Duration;

    use futures::StreamExt;
    use log::LevelFilter;
    use ton_block::MsgAddressInt;

    use crate::TransactionProducer;

    #[tokio::test]
    async fn test_get() {
        let pr = TransactionProducer::new(
            "test",
            "test".to_string(),
            "http://35.240.13.113:8081",
            Default::default(),
        )
        .unwrap();
        pr.get_contract_state(
            &MsgAddressInt::from_str(
                "0:8e2586602513e99a55fa2be08561469c7ce51a7d5a25977558e77ef2bc9387b4",
            )
            .unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    }
}
