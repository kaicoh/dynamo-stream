use super::Records;

use anyhow::Result;
use serde::Serialize;

#[derive(Debug)]
pub struct Event {
    pub table_name: String,
    pub url: String,
    pub payload: Payload,
}

impl Event {
    pub async fn notify(self) -> Result<()> {
        let url = self.url;
        let table_name = self.table_name;

        match self.payload {
            Payload::Records(records) => send(&url, &records).await,
            Payload::Error(message) => {
                let body = serde_json::json!({
                    "table_name": table_name,
                    "message": message,
                });
                send(&url, &body).await
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum Payload {
    Records(Records),
    Error(String),
}

async fn send<B: Serialize>(url: &str, body: &B) -> Result<()> {
    reqwest::Client::new()
        .post(url)
        .json(body)
        .send()
        .await
        .map(drop)
        .map_err(anyhow::Error::from)
}

impl From<Records> for Payload {
    fn from(records: Records) -> Self {
        Self::Records(records)
    }
}

impl<T: Into<String>> From<T> for Payload {
    fn from(message: T) -> Payload {
        Self::Error(message.into())
    }
}
