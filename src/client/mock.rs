use super::{Client, GetRecordsOutput, GetShardsOutput};

use anyhow::Result;
use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct MockClient;

#[async_trait]
impl Client for MockClient {
    async fn get_shards(&self, _table_name: &str) -> Result<GetShardsOutput> {
        unimplemented!()
    }

    async fn get_records(&self, _iterator: &str) -> Result<GetRecordsOutput> {
        unimplemented!()
    }
}
