use crate::client::Client;

use super::{Event, NotiEvent};

use std::sync::Arc;
use tokio::sync::{
    mpsc,
    oneshot::{self, error::TryRecvError},
};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
use tracing::{error, info, warn};

#[derive(Debug)]
pub struct Subscription {
    table_name: String,
    url: String,
    interval: Option<u64>,
    sender: Option<oneshot::Sender<Event>>,
    receiver: Option<oneshot::Receiver<Event>>,
    notifier: Option<mpsc::Sender<NotiEvent>>,
}

impl Subscription {
    pub fn new<S, T>(
        table_name: S,
        url: T,
        sender: oneshot::Sender<Event>,
        receiver: oneshot::Receiver<Event>,
        notifier: mpsc::Sender<NotiEvent>,
    ) -> Self
    where
        S: Into<String>,
        T: Into<String>,
    {
        Self {
            table_name: table_name.into(),
            url: url.into(),
            interval: None,
            sender: Some(sender),
            receiver: Some(receiver),
            notifier: Some(notifier),
        }
    }

    pub fn set_interval(self, secs: u64) -> Self {
        Self {
            interval: Some(secs),
            ..self
        }
    }

    pub fn start_polling(&mut self, client: Arc<dyn Client>) -> JoinHandle<()> {
        let table_name = self.table_name.clone();
        let url = self.url.clone();
        let interval = self.interval.take();
        let tx = self.sender.take().expect("sender is None");
        let mut rx = self.receiver.take().expect("receiver is None");
        let notifier = self.notifier.take().expect("notifier is None");

        tokio::spawn(async move {
            let oneshot_send = oneshot_sender(tx);

            let mut shards = match client.get_shards(&table_name).await {
                Ok(output) => output.shards,
                Err(err) => {
                    let event = Event::new_err(
                        err,
                        format!("Failed to get shards. table_name: {table_name}"),
                    );
                    oneshot_send(event);
                    return;
                }
            };

            if shards.is_empty() {
                let event = Event::Error {
                    message: format!("No shards in `{table_name}`"),
                    error: anyhow::anyhow!("Empty shards"),
                };
                oneshot_send(event);
                return;
            }

            loop {
                if let Some(secs) = interval {
                    sleep(Duration::from_secs(secs)).await;
                }

                match rx.try_recv() {
                    Ok(event) => {
                        info!("Got event `{event}`. Stopping polling process.");
                        break;
                    }
                    Err(TryRecvError::Closed) => {
                        error!("Oneshot channel is closed unexpectedly.");
                        break;
                    }
                    _ => {}
                }

                let (records, next_shards) = match client.get_records(&shards).await {
                    Ok(output) => (output.records, output.shards),
                    Err(err) => {
                        let event = Event::new_err(err, "Failed to get records");
                        oneshot_send(event);
                        break;
                    }
                };

                if !records.is_empty() {
                    let event = NotiEvent::http(&url, records);
                    // MEMO: Not break the loop even if notification fails
                    if let Err(err) = notifier.send(event).await {
                        warn!("{:#?}", err);
                    }
                }

                if next_shards.is_empty() {
                    oneshot_send(Event::Closed);
                    break;
                }

                shards = next_shards;
            }
        })
    }
}

fn oneshot_sender(tx: oneshot::Sender<Event>) -> impl FnOnce(Event) {
    |event: Event| {
        if let Err(err) = tx.send(event) {
            error!("{:#?}", err);
        }
    }
}
