use crate::stream::client::Client;

use super::{Channel, ChannelEvent, NotiEvent, Subscription};

use serde::Serialize;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Copy, Clone, Serialize, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
pub enum EntryStatus {
    Created,
    Running,
    Closed,
    Removed,
    Error,
}

#[derive(Debug)]
pub struct Entry {
    table_name: String,
    url: String,
    status: EntryStatus,
    error: Option<String>,
    channel: Channel,
    subscription: Subscription,
}

impl Entry {
    pub fn new<S, T>(table_name: S, url: T, notifier: mpsc::Sender<NotiEvent>) -> Self
    where
        S: Into<String>,
        T: Into<String>,
    {
        let table_name: String = table_name.into();
        let url: String = url.into();

        let (tx_0, rx_0) = oneshot::channel::<ChannelEvent>();
        let (tx_1, rx_1) = oneshot::channel::<ChannelEvent>();

        let channel = Channel::new(tx_0, rx_1);
        let subscription =
            Subscription::new(&table_name, &url, tx_1, rx_0, notifier).set_interval(3);

        Self {
            table_name,
            url,
            status: EntryStatus::Created,
            error: None,
            channel,
            subscription,
        }
    }

    pub fn table_name(&self) -> &str {
        self.table_name.as_str()
    }

    pub fn url(&self) -> &str {
        self.url.as_str()
    }

    pub fn status(&self) -> EntryStatus {
        self.status
    }

    pub fn set_removed(&mut self) {
        self.status = EntryStatus::Removed;
    }

    pub fn start_polling(&mut self, client: Arc<dyn Client>) {
        self.subscription.start_polling(client);
        self.status = EntryStatus::Running;
    }

    pub fn check_polling(&mut self) {
        let (status, error) = self.channel.poll();
        self.status = status;
        self.error = error;
    }

    pub fn finish_polling(&mut self) {
        self.channel.close();
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct EntryState {
    table_name: String,
    url: String,
    status: EntryStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl EntryState {
    pub fn from(entry: &Entry) -> Self {
        Self {
            table_name: entry.table_name().to_owned(),
            url: entry.url().to_owned(),
            status: entry.status(),
            error: entry.error.clone(),
        }
    }
}
