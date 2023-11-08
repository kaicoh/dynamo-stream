mod dynamodb;
#[cfg(test)]
mod mock;

use crate::types::{Record, Shard};

use anyhow::Result;
use axum::async_trait;

#[derive(Debug, Clone)]
pub struct GetShardsOutput {
    pub shards: Vec<Shard>,
}

#[derive(Debug, Clone)]
pub struct GetRecordsOutput {
    pub next_iterator: Option<String>,
    pub records: Vec<Record>,
}

#[async_trait]
pub trait Client: Send + Sync {
    async fn get_shards(&self, table_name: &str) -> Result<GetShardsOutput>;
    async fn get_records(&self, iterator: &str) -> Result<GetRecordsOutput>;
}

pub use dynamodb::DynamodbClient;
#[cfg(test)]
pub use mock::MockClient;
