mod builder;

use super::{
    client::{Client, GetShardsOutput},
    lineages::Lineages,
    shard::Shard,
    types::Records,
    Event, ReceiverHalf, SenderHalf, Stream,
};

use anyhow::Result;
use axum::async_trait;
use std::sync::Arc;
use tokio::sync::{oneshot, watch};
use tracing::error;

pub use builder::{DynamodbStreamBuilder, DynamodbStreamHalf};

pub struct DynamodbStream {
    client: Arc<dyn Client>,
    arn: String,
    table: String,
    rx_event: oneshot::Receiver<Event>,
    tx_records: watch::Sender<Records>,
    shards: Vec<Shard>,
}

impl DynamodbStream {
    pub fn builder() -> DynamodbStreamBuilder {
        DynamodbStreamBuilder::new()
    }

    fn client(&self) -> Arc<dyn Client> {
        Arc::clone(&self.client)
    }
}

impl ReceiverHalf for DynamodbStream {
    fn rx_event(&mut self) -> &mut oneshot::Receiver<Event> {
        &mut self.rx_event
    }
}

#[async_trait]
impl Stream for DynamodbStream {
    fn table_name(&self) -> &str {
        self.table.as_str()
    }

    fn tx_records(&self) -> &watch::Sender<Records> {
        &self.tx_records
    }

    async fn init(&mut self) -> Result<()> {
        let arn = self.client.get_stream_arn(&self.table).await?.stream_arn;
        let shards = get_all_shards(Arc::clone(&self.client), &arn).await?;
        let shards = set_shard_iterators(Arc::clone(&self.client), &arn, shards).await;

        self.arn = arn;
        self.shards = shards;

        Ok(())
    }

    async fn iterate(&mut self) -> Result<Records> {
        // Get records from current shards.
        let mut shards: Vec<Shard> = vec![];
        shards.append(&mut self.shards);

        let lineages = Lineages::from(shards);
        let (mut records, mut shards) = lineages.get_records(self.client()).await;

        // Refresh shards
        // 1. Get shards which the stream doesn't have.
        let new_shards = get_all_shards(self.client(), &self.arn)
            .await?
            .into_iter()
            .filter(|shard| !shards.iter().any(|s| s.id() == shard.id()))
            .collect::<Vec<Shard>>();

        // 2. Set iterators to new shards.
        let mut new_shards = set_shard_iterators(self.client(), &self.arn, new_shards).await;

        // 3. Append new shards
        shards.append(&mut new_shards);
        self.shards = shards;

        records.sort();
        Ok(records)
    }
}

async fn get_all_shards(client: Arc<dyn Client>, stream_arn: &str) -> Result<Vec<Shard>> {
    let GetShardsOutput {
        mut shards,
        mut last_shard_id,
    } = client.get_shards(stream_arn, None).await?;

    while last_shard_id.is_some() {
        let mut output = client.get_shards(stream_arn, last_shard_id.take()).await?;
        shards.append(&mut output.shards);
        last_shard_id = output.last_shard_id;
    }

    Ok(shards)
}

async fn set_shard_iterators(
    client: Arc<dyn Client>,
    stream_arn: &str,
    shards: Vec<Shard>,
) -> Vec<Shard> {
    let mut output: Vec<Shard> = vec![];
    let buf_size = channel_size(&shards);
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Shard>(buf_size);

    for mut shard in shards {
        let client = Arc::clone(&client);
        let tx = tx.clone();
        let arn = stream_arn.to_string();

        tokio::spawn(async move {
            if let Err(err) = shard.set_iterator(client, &arn).await {
                error!("Failed to get shard iterator: {err}");
                error!("{:#?}", err);
                return;
            }

            // Send shard only if the set_iterator method succeeds
            if let Err(err) = tx.send(shard).await {
                error!("Failed to send shard: {err}");
                error!("{:#?}", err);
            }
        });
    }

    drop(tx);

    while let Some(shard) = rx.recv().await {
        // Push shard having its iterator
        output.push(shard);
    }

    output
}

fn channel_size(shards: &[Shard]) -> usize {
    if shards.is_empty() {
        1
    } else {
        shards.len()
    }
}
