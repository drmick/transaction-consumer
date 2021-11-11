use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use futures::channel::oneshot;
use futures::{SinkExt, Stream};
use nekoton::transport::models::ExistingContract;
use nekoton_utils::SimpleClock;
use rdkafka::config::FromClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message};
use reqwest::StatusCode;
use serde::Serialize;
use ton_block::{Deserializable, MsgAddressInt};
use ton_block_compressor::ZstdWrapper;
use ton_types::UInt256;
use url::Url;

pub struct TransactionProducer {
    consumer: StreamConsumer,
    states_url: Url,
    states_client: reqwest::Client,
    topic: String,
}

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
            .set("auto.offset.reset", "earliest");

        for (k, v) in options {
            config.set(k, v);
        }
        let states_rpc_endpoint =
            Url::parse(states_rpc_endpoint.as_ref()).context("Bad rpc endpoint")?;
        Ok(Arc::new(Self {
            consumer: StreamConsumer::from_config(&config)?,
            states_url: states_rpc_endpoint,
            states_client: Default::default(),
            topic,
        }))
    }

    pub async fn stream_blocks(self: Arc<Self>) -> Result<impl Stream<Item = ProducedTransaction>> {
        self.consumer.subscribe(&[&self.topic])?;

        let (mut tx, rx) = futures::channel::mpsc::channel(1);
        let this = self;
        tokio::spawn(async move {
            let mut decompressor = ZstdWrapper::new();
            while let Ok(a) = this.consumer.recv().await {
                let payload = try_opt!(a.payload(), "no payload");
                let payload_decompressed = try_res!(
                    decompressor.decompress(payload),
                    "Failed decompressing block data"
                );
                let transaction = try_res!(
                    ton_block::Transaction::construct_from_bytes(payload_decompressed),
                    "Failed constructing block"
                );

                let key = try_opt!(a.key(), "No key");
                let key = UInt256::from_slice(key);

                let (block, rx) = ProducedTransaction::new(key, transaction);
                if let Err(e) = tx.send(block).await {
                    log::error!("Failed sending via channel: {:?}", e);
                    return;
                }
                if let Err(e) = rx.await {
                    log::warn!("Committer is dropped: {}", e);
                    continue;
                } //waiting for commit
                if let Err(e) = this.consumer.commit_consumer_state(CommitMode::Async) {
                    log::error!("Failed committing: {:?}", e);
                    return;
                }
            }
        });
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
            let bytes = response.bytes().await.context("Failed getting raw data")?;
            Ok(Some(bincode::deserialize(bytes.as_ref())?))
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

#[cfg(test)]
mod test {
    use crate::ProducedTransaction;

    #[tokio::test]
    async fn test() {
        let (mut produced_block, rx) =
            ProducedTransaction::new(Default::default(), Default::default());
        produced_block.commit().unwrap();
        rx.await.unwrap();
    }
}
