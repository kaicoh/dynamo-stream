use super::Records;

use axum::async_trait;
use std::hash::Hash;

#[async_trait]
pub trait Consumer: Hash + Eq + Send + Sync {
    /// Consume dynamodb stream
    async fn consume(&self, records: Records) -> ();
}
