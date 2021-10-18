use std::sync::Arc;

use anyhow::{Context, Result};
use futures::channel::oneshot;
use futures::{SinkExt, Stream};
use rdkafka::config::FromClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message};
use ton_block::Deserializable;
use ton_block_compressor::ZstdWrapper;
use ton_types::UInt256;

pub struct BlockProducer {
    consumer: StreamConsumer,
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

pub struct ProducedBlock {
    id: UInt256,
    block: ton_block::Block,
    commit_channel: Option<oneshot::Sender<()>>,
}

impl ProducedBlock {
    fn new(id: UInt256, block: ton_block::Block) -> (Self, oneshot::Receiver<()>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                id,
                block,
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

    pub fn inner(&mut self) -> (UInt256, ton_block::Block) {
        (
            std::mem::take(&mut self.id),
            std::mem::take(&mut self.block),
        )
    }
}

impl BlockProducer {
    pub fn new(group_id: &str, topic: String) -> Result<Self> {
        let mut config = ClientConfig::default();
        config
            .set("group.id", group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest");
        Ok(Self {
            consumer: StreamConsumer::from_config(&config)?,
            topic,
        })
    }

    pub async fn stream_blocks(self: Arc<Self>) -> Result<impl Stream<Item = ProducedBlock>> {
        self.consumer.subscribe(&[&self.topic])?;

        let (mut tx, rx) = futures::channel::mpsc::channel(1);
        let this = self.clone();
        tokio::spawn(async move {
            let mut decompressor = ZstdWrapper::new();
            while let Ok(a) = this.consumer.recv().await {
                let payload = try_opt!(a.payload(), "no payload");
                let payload_decompressed = try_res!(
                    decompressor.decompress(payload),
                    "Failed decompressing block data"
                );
                let block = try_res!(
                    ton_block::Block::construct_from_bytes(&payload_decompressed),
                    "Failed constructing block"
                );

                let key = try_opt!(a.key(), "No key");
                let key = UInt256::from_slice(key);

                let (block, rx) = ProducedBlock::new(key, block);
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
}

#[cfg(test)]
mod test {
    use crate::ProducedBlock;

    #[tokio::test]
    async fn test() {
        let (mut produced_block, rx) = ProducedBlock::new(Default::default(), Default::default());
        produced_block.commit().unwrap();
        rx.await.unwrap();
    }
}
