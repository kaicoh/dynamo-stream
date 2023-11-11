mod event;

use crate::stream::Records;

pub use event::{Event, Payload};

use anyhow::Result;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::warn;

#[derive(Debug)]
pub struct Notification {
    table_name: String,
    url: String,
    tx: Sender<Event>,
}

impl Notification {
    pub fn new<T, U>(table_name: T, url: U, tx: Sender<Event>) -> Self
    where
        T: Into<String>,
        U: Into<String>,
    {
        Self {
            table_name: table_name.into(),
            url: url.into(),
            tx,
        }
    }

    pub async fn send<P: Into<Payload>>(&self, payload: P) -> Result<()> {
        let event = Event {
            table_name: self.table_name.to_owned(),
            url: self.url.to_owned(),
            payload: payload.into(),
        };

        self.tx.send(event).await.map_err(anyhow::Error::from)
    }
}

pub async fn start(mut receiver: Receiver<Event>) -> Result<()> {
    while let Some(event) = receiver.recv().await {
        tokio::spawn(async move {
            if let Err(err) = event.notify().await {
                warn!("{:#?}", err);
            }
        });
    }

    Err(anyhow::anyhow!(
        "It seems all event senders have been dropped"
    ))
}
