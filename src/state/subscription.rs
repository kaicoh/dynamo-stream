use crate::stream::client::Client;

use super::{ChannelEvent, NotiEvent};

use std::sync::Arc;
use tokio::sync::{
    mpsc,
    oneshot::{self, error::TryRecvError},
};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
use tracing::{error, info};

#[derive(Debug)]
pub struct Subscription {
    table_name: String,
    url: String,
    interval: Option<u64>,
    sender: Option<oneshot::Sender<ChannelEvent>>,
    receiver: Option<oneshot::Receiver<ChannelEvent>>,
    notifier: Option<mpsc::Sender<NotiEvent>>,
}

impl Subscription {
    pub fn new<S, T>(
        table_name: S,
        url: T,
        sender: oneshot::Sender<ChannelEvent>,
        receiver: oneshot::Receiver<ChannelEvent>,
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
                Err(error) => {
                    let event = ChannelEvent::Error {
                        message: format!("Failed to get shards. table_name: {table_name}"),
                        error,
                    };
                    oneshot_send(event);
                    notify_err(notifier).await;
                    return;
                }
            };

            if shards.is_empty() {
                let event = ChannelEvent::Error {
                    message: format!("No shards in `{table_name}`"),
                    error: anyhow::anyhow!("Empty shards"),
                };
                oneshot_send(event);
                notify_err(notifier).await;
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
                        notify_err(notifier).await;
                        break;
                    }
                    _ => {}
                }

                let (records, next_shards) = match client.get_records(&shards).await {
                    Ok(output) => (output.records, output.shards),
                    Err(error) => {
                        let event = ChannelEvent::Error {
                            message: "Failed to get records".into(),
                            error,
                        };
                        oneshot_send(event);
                        notify_err(notifier).await;
                        break;
                    }
                };

                if !records.is_empty() {
                    let event = NotiEvent::http(&url, records);

                    if let Err(err) = notifier.send(event).await {
                        oneshot_send(err.into());
                        break;
                    }
                }

                if next_shards.is_empty() {
                    oneshot_send(ChannelEvent::Closed);
                    break;
                }

                shards = next_shards;
            }
        })
    }
}

fn oneshot_sender(tx: oneshot::Sender<ChannelEvent>) -> impl FnOnce(ChannelEvent) {
    |event: ChannelEvent| {
        if let Err(err) = tx.send(event) {
            error!("{:#?}", err);
        }
    }
}

async fn notify_err(tx: mpsc::Sender<NotiEvent>) {
    let event = NotiEvent::error("Server error occurred. Stop subscription.");
    if let Err(err) = tx.send(event).await {
        error!("{:#?}", err);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::client::{RecordsSource, ShardsSource, SourceClient};
    use std::sync::Mutex;

    #[tokio::test]
    async fn it_sends_records_to_notify() {
        let mut s = ShardsSource::new("People");
        s.push([("shard_0", "iterator_0")]);
        let s = Arc::new(Mutex::new(s));

        let mut r = RecordsSource::new();
        r.push(["record_0", "record_1"]);
        let r = Arc::new(Mutex::new(r));

        let client = SourceClient::new(Arc::clone(&s), Arc::clone(&r));
        let (_tx, mut rx, mut noti, mut sub) = build_subscription();

        let result = sub.start_polling(Arc::new(client)).await;
        assert!(result.is_ok());

        match rx.try_recv() {
            Ok(event) => {
                matches!(event, ChannelEvent::Closed);
            }
            Err(_) => {
                unreachable!("Receiver got unexpected event");
            }
        }

        let opt = noti.recv().await;
        assert!(opt.is_some());
        match opt.unwrap() {
            NotiEvent::Http { url, records } => {
                assert_eq!(url, "http://foo.bar");
                assert_eq!(records.len(), 2);
                assert!(records.includes("record_0"));
                assert!(records.includes("record_1"));
            }
            _ => {
                unreachable!("Notification receiver got unexpected event");
            }
        }

        let opt = noti.recv().await;
        assert!(opt.is_none());
    }

    #[tokio::test]
    async fn it_sends_records_until_the_active_shards_are_empty() {
        let mut s = ShardsSource::new("People");
        s.push([("shard_0", "iterator_0"), ("shard_1", "iterator_1")]);
        s.push([("shard_2", "iterator_2")]);
        let s = Arc::new(Mutex::new(s));

        let mut r = RecordsSource::new();
        r.push(["record_0", "record_1"]);
        r.push(["record_2", "record_3", "record_4"]);
        r.push(["record_5"]);
        let r = Arc::new(Mutex::new(r));

        let client = SourceClient::new(Arc::clone(&s), Arc::clone(&r));
        let (_tx, mut rx, mut noti, mut sub) = build_subscription();

        let result = sub.start_polling(Arc::new(client)).await;
        assert!(result.is_ok());

        match rx.try_recv() {
            Ok(event) => {
                matches!(event, ChannelEvent::Closed);
            }
            Err(_) => {
                unreachable!("Receiver got unexpected event");
            }
        }

        let opt = noti.recv().await;
        assert!(opt.is_some());
        match opt.unwrap() {
            NotiEvent::Http { url, records } => {
                assert_eq!(url, "http://foo.bar");
                assert_eq!(records.len(), 2);
                assert!(records.includes("record_0"));
                assert!(records.includes("record_1"));
            }
            _ => {
                unreachable!("Notification receiver got unexpected event");
            }
        }

        let opt = noti.recv().await;
        assert!(opt.is_some());
        match opt.unwrap() {
            NotiEvent::Http { url, records } => {
                assert_eq!(url, "http://foo.bar");
                assert_eq!(records.len(), 3);
                assert!(records.includes("record_2"));
                assert!(records.includes("record_3"));
                assert!(records.includes("record_4"));
            }
            _ => {
                unreachable!("Notification receiver got unexpected event");
            }
        }

        let opt = noti.recv().await;
        assert!(opt.is_none());
    }

    #[tokio::test]
    async fn it_sends_error_if_there_are_no_shards() {
        let s = ShardsSource::new("People");
        let s = Arc::new(Mutex::new(s));

        let mut r = RecordsSource::new();
        r.push(["record_0", "record_1"]);
        let r = Arc::new(Mutex::new(r));

        let client = SourceClient::new(Arc::clone(&s), Arc::clone(&r));
        let (_tx, mut rx, mut noti, mut sub) = build_subscription();

        let result = sub.start_polling(Arc::new(client)).await;
        assert!(result.is_ok());

        let event = rx.try_recv().expect("An event should be sent");
        match event {
            ChannelEvent::Error { message, error } => {
                assert_eq!(message, "No shards in `People`");
                assert_eq!(format!("{error}"), "Empty shards");
            }
            _ => {
                unreachable!("Receiver got unexpected event");
            }
        }

        let opt = noti.recv().await;
        assert!(opt.is_some());
        match opt.unwrap() {
            NotiEvent::Error(message) => {
                assert_eq!(message, "Server error occurred. Stop subscription.");
            }
            _ => {
                unreachable!("Notification receiver got unexpected event");
            }
        }

        let opt = noti.recv().await;
        assert!(opt.is_none());
    }

    #[tokio::test]
    async fn it_stops_sending_when_receiving_event() {
        let mut s = ShardsSource::new("People");
        s.push([("shard_0", "iterator_0")]);
        let s = Arc::new(Mutex::new(s));

        let mut r = RecordsSource::new();
        r.push(["record_0", "record_1"]);
        let r = Arc::new(Mutex::new(r));

        let client = SourceClient::new(Arc::clone(&s), Arc::clone(&r));
        let (tx, mut rx, mut noti, mut sub) = build_subscription();

        tx.send(ChannelEvent::Closed).expect("send should succeed");

        let result = sub.start_polling(Arc::new(client)).await;
        assert!(result.is_ok());

        match rx.try_recv() {
            Err(TryRecvError::Closed) => {}
            _ => {
                unreachable!("Receiver got unexpected event");
            }
        }

        let opt = noti.recv().await;
        assert!(opt.is_none());
    }

    #[tokio::test]
    async fn it_stops_sending_when_the_channel_half_is_dropped() {
        let mut s = ShardsSource::new("People");
        s.push([("shard_0", "iterator_0")]);
        let s = Arc::new(Mutex::new(s));

        let mut r = RecordsSource::new();
        r.push(["record_0", "record_1"]);
        let r = Arc::new(Mutex::new(r));

        let client = SourceClient::new(Arc::clone(&s), Arc::clone(&r));
        let (tx, mut rx, mut noti, mut sub) = build_subscription();

        drop(tx);

        let result = sub.start_polling(Arc::new(client)).await;
        assert!(result.is_ok());

        match rx.try_recv() {
            Err(TryRecvError::Closed) => {}
            _ => {
                unreachable!("Receiver got unexpected event");
            }
        }

        let opt = noti.recv().await;
        assert!(opt.is_some());
        match opt.unwrap() {
            NotiEvent::Error(message) => {
                assert_eq!(message, "Server error occurred. Stop subscription.");
            }
            _ => {
                unreachable!("Notification receiver got unexpected event");
            }
        }

        let opt = noti.recv().await;
        assert!(opt.is_none());
    }

    #[tokio::test]
    async fn it_does_not_send_any_records_when_the_records_are_empty() {
        let mut s = ShardsSource::new("People");
        s.push([("shard_0", "iterator_0")]);
        let s = Arc::new(Mutex::new(s));

        let r = RecordsSource::new();
        let r = Arc::new(Mutex::new(r));

        let client = SourceClient::new(Arc::clone(&s), Arc::clone(&r));
        let (_tx, mut rx, mut noti, mut sub) = build_subscription();

        let result = sub.start_polling(Arc::new(client)).await;
        assert!(result.is_ok());

        match rx.try_recv() {
            Ok(event) => {
                matches!(event, ChannelEvent::Closed);
            }
            Err(_) => {
                unreachable!("Receiver got unexpected event");
            }
        }

        let opt = noti.recv().await;
        assert!(opt.is_none());
    }

    #[tokio::test]
    async fn it_sends_error_if_it_fails_to_notify() {
        let mut s = ShardsSource::new("People");
        s.push([("shard_0", "iterator_0")]);
        let s = Arc::new(Mutex::new(s));

        let mut r = RecordsSource::new();
        r.push(["record_0", "record_1"]);
        let r = Arc::new(Mutex::new(r));

        let client = SourceClient::new(Arc::clone(&s), Arc::clone(&r));
        let (_tx, mut rx, noti, mut sub) = build_subscription();

        drop(noti);

        let result = sub.start_polling(Arc::new(client)).await;
        assert!(result.is_ok());

        let event = rx.try_recv().expect("An event should be sent");
        assert!(matches!(event, ChannelEvent::Error { .. }));
    }

    fn build_subscription() -> (
        oneshot::Sender<ChannelEvent>,
        oneshot::Receiver<ChannelEvent>,
        mpsc::Receiver<NotiEvent>,
        Subscription,
    ) {
        let (tx_0, rx_0) = oneshot::channel::<ChannelEvent>();
        let (tx_1, rx_1) = oneshot::channel::<ChannelEvent>();
        let (tx_2, rx_2) = mpsc::channel::<NotiEvent>(10);

        let subscription = Subscription::new("People", "http://foo.bar", tx_0, rx_1, tx_2);

        (tx_1, rx_0, rx_2, subscription)
    }
}
