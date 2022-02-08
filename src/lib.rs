use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::channel::oneshot;
use futures::{SinkExt, Stream};
use nekoton::transport::models::ExistingContract;
use nekoton_utils::SimpleClock;
use rdkafka::config::FromClientConfig;
use rdkafka::consumer::{Consumer, ConsumerContext, StreamConsumer};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use reqwest::StatusCode;
use serde::Serialize;
use ton_block::{Deserializable, MsgAddressInt};
use ton_block_compressor::ZstdWrapper;
use ton_types::UInt256;
use url::Url;

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

#[derive(Debug, Clone)]
pub struct StatesClient {
    client: reqwest::Client,
    url: Url,
}

impl StatesClient {
    pub fn new<U>(states_rpc_endpoint: U) -> Result<StatesClient>
    where
        U: AsRef<str>,
    {
        let client = reqwest::Client::new();
        let states_rpc_endpoint =
            Url::parse(states_rpc_endpoint.as_ref()).context("Bad rpc endpoint")?;
        Ok(Self {
            client,
            url: states_rpc_endpoint.join("account")?,
        })
    }

    pub async fn get_contract_state(
        &self,
        contract_address: &MsgAddressInt,
    ) -> Result<Option<ExistingContract>> {
        #[derive(Serialize)]
        struct Request {
            address: String,
        }

        let req = Request {
            address: contract_address.to_string(),
        };

        let response = self
            .client
            .post(self.url.clone()) //todo improve?
            .json(&req)
            .send()
            .await
            .context("Failed sending request")?;

        if let StatusCode::OK = response.status() {
            let response: Option<ExistingContract> =
                response.json().await.context("Failed parsing")?;
            Ok(response)
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
            .run_local(&SimpleClock, state.account, input)
            .map(Some)
    }
}

pub struct TransactionProducer {
    consumer: StreamConsumer,
    states_client: StatesClient,
    topic: String,
    subscribed: AtomicBool,
}

impl TransactionProducer {
    pub fn new<U>(
        group_id: &str,
        topic: &str,
        states_rpc_endpoint: U,
        options: HashMap<&str, &str>,
    ) -> Result<Arc<Self>>
    where
        U: AsRef<str>,
    {
        let mut config = ClientConfig::default();
        config
            .set("group.id", group_id)
            .set("enable.auto.commit", "true")
            .set("auto.commit.interval.ms", "5000")
            .set("enable.auto.offset.store", "false")
            .set("auto.offset.reset", "latest");

        for (k, v) in options {
            config.set(k, v);
        }
        let consumer = StreamConsumer::from_config(&config)?;

        Ok(Arc::new(Self {
            consumer,
            states_client: StatesClient::new(states_rpc_endpoint)?,
            topic: topic.to_string(),
            subscribed: AtomicBool::new(false),
        }))
    }

    /// BLOCKING FUNCTION
    /// Resets all partitions to the beginning
    pub fn reset_offsets(&self) -> Result<()> {
        self.subscribe(Offset::Beginning)
    }

    fn subscribe(&self, offset: Offset) -> Result<()> {
        if !self.subscribed.load(Ordering::Acquire) {
            let num_partitions = get_topic_partitions_count(&self.consumer, &self.topic)?;
            let mut assignment = TopicPartitionList::new();
            for x in 0..num_partitions {
                assignment.add_partition_offset(&self.topic, x as i32, offset)?;
            }
            log::info!("Assigning: {:?}", assignment);
            self.consumer.assign(&assignment)?;
            self.subscribed.store(true, Ordering::Release);
        }
        Ok(())
    }

    pub async fn stream_transactions(
        self: Arc<Self>,
    ) -> Result<impl Stream<Item = ProducedTransaction>> {
        self.subscribe(Offset::Stored)?;

        let (mut tx, rx) = futures::channel::mpsc::channel(1);

        let this = self;
        log::info!("Statrting streaming");
        tokio::spawn(async move {
            let mut decompressor = ZstdWrapper::new();

            while let Ok(message) = this.consumer.recv().await {
                let payload = try_opt!(message.payload(), "no payload");
                let payload_decompressed = try_res!(
                    decompressor.decompress(payload),
                    "Failed decompressing block data"
                );
                tokio::task::yield_now().await;
                let transaction = try_res!(
                    ton_block::Transaction::construct_from_bytes(payload_decompressed),
                    "Failed constructing block"
                );

                let key = try_opt!(message.key(), "No key");
                let key = UInt256::from_slice(key);

                let (block, rx) = ProducedTransaction::new(key, transaction);
                if let Err(e) = tx.send(block).await {
                    log::error!("Failed sending via channel: {:?}", e); //todo panic?
                    return;
                }

                if rx.await.is_err() {
                    continue;
                }

                try_res!(
                    this.consumer.store_offset_from_message(&message),
                    "Failed committing"
                );
                log::debug!("Stored offsets");
            }
        });
        Ok(rx)
    }

    pub async fn get_contract_state(
        &self,
        contract_address: &MsgAddressInt,
    ) -> Result<Option<ExistingContract>> {
        self.states_client
            .get_contract_state(contract_address)
            .await
    }

    pub async fn run_local(
        &self,
        contract_address: &MsgAddressInt,
        function: &ton_abi::Function,
        input: &[ton_abi::Token],
    ) -> Result<Option<nekoton_abi::ExecutionOutput>> {
        self.states_client
            .run_local(contract_address, function, input)
            .await
    }

    pub fn get_client(&self) -> &StatesClient {
        &self.states_client
    }
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

fn get_topic_partitions_count<X: ConsumerContext, C: Consumer<X>>(
    consumer: &C,
    topic_name: &str,
) -> Result<usize> {
    let metadata = consumer
        .fetch_metadata(Some(topic_name), Duration::from_secs(30))
        .context("Failed to fetch metadata")?;

    if metadata.topics().is_empty() {
        anyhow::bail!("Topics is empty")
    } else {
        let partitions = metadata
            .topics()
            .iter()
            .find(|x| x.name() == topic_name)
            .map(|x| x.partitions().iter().count())
            .context("No such topic")?;
        Ok(partitions)
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use ton_block::MsgAddressInt;

    use crate::StatesClient;

    #[tokio::test]
    async fn test_get() {
        let pr = StatesClient::new("http://35.240.13.113:8081").unwrap();

        pr.get_contract_state(
            &MsgAddressInt::from_str(
                "0:8e2586602513e99a55fa2be08561469c7ce51a7d5a25977558e77ef2bc9387b4",
            )
            .unwrap(),
        )
        .await
        .unwrap()
        .unwrap();

        pr.get_contract_state(
            &MsgAddressInt::from_str(
                "-1:efd5a14409a8a129686114fc092525fddd508f1ea56d1b649a3a695d3a5b188c",
            )
            .unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(pr
            .get_contract_state(
                &MsgAddressInt::from_str(
                    "-1:aaa5a14409a8a129686114fc092525fddd508f1ea56d1b649a3a695d3a5b188c",
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .is_none());
    }
}
