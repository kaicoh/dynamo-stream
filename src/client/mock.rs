use super::{Client, GetRecordsOutput, GetShardsOutput, Shards};

use anyhow::Result;
use axum::async_trait;

#[derive(Debug, Clone)]
pub struct MockClient;

#[async_trait]
impl Client for MockClient {
    async fn get_shards(&self, _table_name: &str) -> Result<GetShardsOutput> {
        unimplemented!()
    }

    async fn get_records(&self, _shards: &Shards) -> Result<GetRecordsOutput> {
        unimplemented!()
    }
}
