use super::{NotiEvent, NotiEventFactory, Records};

use anyhow::Result;
use tokio::sync::mpsc::Sender;

#[derive(Debug)]
pub struct Notification {
    factory: NotiEventFactory,
    sender: Sender<NotiEvent>,
}

impl Notification {
    pub fn new(table_name: &str, url: &str, sender: Sender<NotiEvent>) -> Self {
        Self {
            factory: NotiEvent::factory(table_name, url),
            sender,
        }
    }

    pub async fn send_records(&self, records: Records) -> Result<()> {
        let event = self.factory.records(records);
        self.send(event).await
    }

    pub async fn send_error(&self) -> Result<()> {
        let event = self
            .factory
            .error("Server error occurred. Stop subscription.".into());
        self.send(event).await
    }

    async fn send(&self, event: NotiEvent) -> Result<()> {
        self.sender.send(event).await.map_err(anyhow::Error::from)
    }
}
