mod dynamodb;
#[cfg(test)]
mod mock;

use crate::types::{Record, Records, Shards};

use anyhow::Result;
use axum::async_trait;

#[derive(Debug, Clone)]
pub struct GetShardsOutput {
    pub shards: Shards,
}

#[derive(Debug, Clone)]
pub struct GetRecordsOutput {
    pub shards: Shards,
    pub records: Records,
}

#[async_trait]
pub trait Client: Send + Sync {
    async fn get_shards(&self, table_name: &str) -> Result<GetShardsOutput>;
    async fn get_records(&self, shards: &Shards) -> Result<GetRecordsOutput>;
}

pub use dynamodb::DynamodbClient;
#[cfg(test)]
pub use mock::MockClient;
