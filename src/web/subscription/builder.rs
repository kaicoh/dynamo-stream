use super::super::{Client, DynamodbStream, Stream};
use super::*;

use anyhow::Result;
use std::sync::Arc;

#[derive(Default)]
pub struct SubscriptionBuilder {
    client: Option<Arc<dyn Client>>,
    table: Option<String>,
}

impl SubscriptionBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_client(self, client: Arc<dyn Client>) -> Self {
        Self {
            client: Some(client),
            ..self
        }
    }

    pub fn set_table<T: Into<String>>(self, table: T) -> Self {
        Self {
            table: Some(table.into()),
            ..self
        }
    }

    pub async fn build(self) -> Result<Subscription> {
        assert!(self.client.is_some(), "\"client\" is not set");
        assert!(self.table.is_some(), "\"table\" is not set");

        let client = self.client.unwrap();
        let table = self.table.unwrap();

        let (mut stream, stream_half) = DynamodbStream::builder()
            .set_client(client)
            .set_table(&table)
            .build()
            .await?;

        tokio::spawn(async move {
            stream.start_streaming(Some(3)).await;
        });

        Ok(Subscription {
            table,
            destinations: HashMap::new(),
            stream_half,
            listener_halfs: HashMap::new(),
        })
    }
}
