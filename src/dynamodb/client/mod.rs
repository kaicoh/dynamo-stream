mod dynamodb;

use super::shard::Shard;
use super::types::{Records, Record};

use anyhow::Result;
use axum::async_trait;

#[derive(Debug)]
pub struct GetIteratorOutput {
    pub iterator: Option<String>,
}

#[derive(Debug)]
pub struct GetRecordsOutput {
    pub records: Records,
    pub next_iterator: Option<String>,
}

#[derive(Debug)]
pub struct GetShardsOutput {
    pub shards: Vec<Shard>,
    pub last_shard_id: Option<String>,
}

#[derive(Debug)]
pub struct GetStreamArnOutput {
    pub stream_arn: String,
}

#[async_trait]
pub trait Client: Send + Sync {
    async fn get_iterator(&self, stream_arn: &str, shard_id: &str) -> Result<GetIteratorOutput>;
    async fn get_records(&self, iterator: &str) -> Result<GetRecordsOutput>;
    async fn get_shards(&self, arn: &str, exclusive_shard_id: Option<String>) -> Result<GetShardsOutput>;
    async fn get_stream_arn(&self, table: &str) -> Result<GetStreamArnOutput>;
}
