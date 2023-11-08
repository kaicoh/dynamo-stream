use crate::client::Client;
use crate::types::{Record, Records, Shard};

use anyhow::Result;
use serde::Serialize;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use tokio::sync::oneshot::{self, error::TryRecvError, Receiver, Sender};
use tokio::time::{sleep, Duration};
use tracing::{error, info, warn};

type Shards = HashMap<String, Shard>;
type ChildResult<T> = std::result::Result<T, ChildEvent>;

#[derive(Debug, Copy, Clone, Serialize, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
pub enum Status {
    Created,
    Running,
    Closed,
    Removed,
    Error,
}

#[derive(Debug, Copy, Clone)]
pub enum ParentEvent {
    Close,
    Removed,
}

impl fmt::Display for ParentEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParentEvent::Close => write!(f, "CLOSE"),
            ParentEvent::Removed => write!(f, "REMOVED"),
        }
    }
}

#[derive(Debug)]
pub enum ChildEvent {
    ShardIteratorsClosed,
    Error {
        message: String,
        error: anyhow::Error,
    },
}

#[derive(Debug)]
pub struct Entry {
    table_name: String,
    url: String,
    status: Status,
    error: Option<String>,
    sender: Option<Sender<ParentEvent>>,
    receiver: Option<Receiver<ChildEvent>>,
}

impl Entry {
    pub fn new<S, T>(table_name: S, url: T) -> Self
    where
        S: Into<String>,
        T: Into<String>,
    {
        Self {
            table_name: table_name.into(),
            url: url.into(),
            status: Status::Created,
            error: None,
            sender: None,
            receiver: None,
        }
    }

    pub fn table_name(&self) -> &str {
        self.table_name.as_str()
    }

    pub fn url(&self) -> &str {
        self.url.as_str()
    }

    pub fn status(&self) -> Status {
        self.status
    }

    pub fn mark_removed(&mut self) {
        self.status = Status::Removed;
    }

    pub fn start_polling(&mut self, client: Arc<dyn Client>) {
        let client = Arc::clone(&client);
        let table_name = self.table_name().to_owned();
        let url = self.url().to_owned();

        let (tx_to_child, mut rx_from_parent) = oneshot::channel::<ParentEvent>();
        let (tx_to_parent, rx_from_child) = oneshot::channel::<ChildEvent>();

        tokio::spawn(async move {
            let mut shards = match get_shards(&client, &table_name).await {
                Ok(shards) => shards,
                Err(event) => {
                    if let Err(err) = tx_to_parent.send(event) {
                        error!("{:#?}", err);
                    }
                    return;
                }
            };

            if shards.is_empty() {
                let event = ChildEvent::Error {
                    message: format!("No shards in `{table_name}`"),
                    error: anyhow::anyhow!("Empty shards"),
                };
                if let Err(err) = tx_to_parent.send(event) {
                    error!("{:#?}", err);
                }
                return;
            }

            loop {
                sleep(Duration::from_secs(3)).await;

                match rx_from_parent.try_recv() {
                    Ok(event) => {
                        info!("Got event `{event}` from parent. Stopping polling process.");
                        break;
                    }
                    Err(TryRecvError::Closed) => {
                        error!("Oneshot channel is closed unexpectedly.");
                        break;
                    }
                    _ => {}
                }

                let (records, next_shards) = match get_records(&client, &shards).await {
                    Ok(values) => values,
                    Err(event) => {
                        if let Err(err) = tx_to_parent.send(event) {
                            error!("{:#?}", err);
                        }
                        break;
                    }
                };

                if !records.is_empty() {
                    // MEMO: Not break the loop even if notification fails
                    if let Err(err) = notify(&url, records).await {
                        warn!("{:#?}", err);
                    }
                }

                if next_shards.is_empty() {
                    if let Err(err) = tx_to_parent.send(ChildEvent::ShardIteratorsClosed) {
                        error!("{:#?}", err);
                    }
                    break;
                }

                shards = next_shards;
            }
        });

        self.sender = Some(tx_to_child);
        self.receiver = Some(rx_from_child);
        self.status = Status::Running;
    }

    pub fn check_polling(&mut self) {
        assert!(self.receiver.is_some(), "Receiver should have some value");

        if let Some(rx) = self.receiver.as_mut() {
            match rx.try_recv() {
                Ok(ChildEvent::ShardIteratorsClosed) => {
                    info!("Shard itrators are all closed");
                    self.close_channels(ParentEvent::Close);
                    self.status = Status::Closed;
                }
                Ok(ChildEvent::Error { message, error }) => {
                    error!("{:#?}", error);
                    self.close_channels(ParentEvent::Close);
                    self.status = Status::Error;
                    self.error = Some(format!("Unexpected error: {message}"));
                }
                Err(TryRecvError::Closed) => {
                    let message = "Oneshot channel is closed unexpectedly.";
                    error!(message);
                    self.close_channels(ParentEvent::Close);
                    self.status = Status::Error;
                    self.error = Some(message.into());
                }
                _ => {}
            }
        }
    }

    pub fn finish_polling(&mut self) {
        self.close_channels(ParentEvent::Removed);
    }

    fn close_channels(&mut self, event: ParentEvent) {
        if let Some(tx) = self.sender.take() {
            if !tx.is_closed() {
                if let Err(err) = tx.send(event) {
                    error!("Failed to send `{:?}` event.", event);
                    error!("{:#?}", err);
                }
            }
        }

        self.receiver = None;
    }
}

async fn get_shards(client: &Arc<dyn Client>, table_name: &str) -> ChildResult<Shards> {
    client
        .get_shards(table_name)
        .await
        .map(|output| {
            let mut shards: Shards = HashMap::new();

            for shard in output.shards {
                let id = shard.id().to_string();
                shards.insert(id, shard);
            }

            shards
        })
        .map_err(|err| ChildEvent::Error {
            message: format!("Failed to get shards. table_name: {table_name}"),
            error: err,
        })
}

async fn get_records(client: &Arc<dyn Client>, shards: &Shards) -> ChildResult<(Records, Shards)> {
    let mut records: Vec<Record> = vec![];
    let mut next_shards: Shards = HashMap::new();

    for (id, shard) in shards.iter() {
        let mut output =
            client
                .get_records(shard.iterator())
                .await
                .map_err(|err| ChildEvent::Error {
                    message: "Failed to get records.".to_string(),
                    error: err,
                })?;

        records.append(&mut output.records);

        if let Some(iterator) = output.next_iterator {
            let new_shard = Shard::new(id, iterator);
            next_shards.insert(id.into(), new_shard);
        }
    }

    Ok((Records::new(records), next_shards))
}

async fn notify(url: &str, records: Records) -> Result<()> {
    info!("{:#?}", records);
    reqwest::Client::new()
        .post(url)
        .json(&records)
        .send()
        .await
        .map(drop)
        .map_err(anyhow::Error::from)
}

#[derive(Debug, Clone, Serialize)]
pub struct ClonableEntry {
    table_name: String,
    url: String,
    status: Status,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl ClonableEntry {
    pub fn new(entry: &Entry) -> Self {
        Self {
            table_name: entry.table_name.clone(),
            url: entry.url.clone(),
            status: entry.status,
            error: entry.error.clone(),
        }
    }
}
