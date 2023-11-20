mod builder;

use super::{Consumer, Event, HandleEvent, Records};

use axum::async_trait;
use tokio::sync::{oneshot, watch};
use tracing::warn;

pub use builder::{ListenerBuilder, ListenerHalf};

#[derive(Debug)]
pub struct Listener {
    url: String,
    tx_event: Option<oneshot::Sender<Event>>,
    rx_event: oneshot::Receiver<Event>,
    rx_records: watch::Receiver<Records>,
}

impl Listener {
    pub fn builder() -> ListenerBuilder {
        ListenerBuilder::new()
    }
}

impl HandleEvent for Listener {
    fn tx_event(&mut self) -> Option<oneshot::Sender<Event>> {
        self.tx_event.take()
    }

    fn rx_event(&mut self) -> &mut oneshot::Receiver<Event> {
        &mut self.rx_event
    }
}

#[async_trait]
impl Consumer for Listener {
    fn identifier(&self) -> &str {
        self.url.as_str()
    }

    fn rx_records(&mut self) -> &mut watch::Receiver<Records> {
        &mut self.rx_records
    }

    async fn consume(&self, records: Records) {
        if records.is_empty() {
            return;
        }

        if let Err(err) = reqwest::Client::new()
            .post(self.url.as_str())
            .json(&records)
            .send()
            .await
        {
            warn!("Failed to send records to {}", self.url);
            warn!("{:#?}", err);
        }
    }
}
