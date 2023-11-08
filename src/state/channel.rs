use super::{ChannelEvent, EntryStatus};

use tokio::sync::oneshot::{error::TryRecvError, Receiver, Sender};
use tracing::{error, info, warn};

#[derive(Debug)]
pub struct Channel {
    sender: Option<Sender<ChannelEvent>>,
    receiver: Receiver<ChannelEvent>,
}

impl Channel {
    pub fn new(sender: Sender<ChannelEvent>, receiver: Receiver<ChannelEvent>) -> Self {
        Self {
            sender: Some(sender),
            receiver,
        }
    }

    pub fn poll(&mut self) -> (EntryStatus, Option<String>) {
        match self.receiver.try_recv() {
            Ok(ChannelEvent::Closed) => {
                info!("Shard itrators are all closed");
                self.close();
                (EntryStatus::Closed, None)
            }
            Ok(ChannelEvent::Error { message, error }) => {
                error!("{:#?}", error);
                self.close();
                (
                    EntryStatus::Error,
                    Some(format!("Unexpected error: {message}")),
                )
            }
            Err(TryRecvError::Closed) => {
                let message = "Oneshot channel is closed unexpectedly.";
                error!(message);
                self.close();
                (EntryStatus::Error, Some(message.into()))
            }
            _ => (EntryStatus::Running, None),
        }
    }

    pub fn close(&mut self) {
        if let Some(tx) = self.sender.take() {
            if !tx.is_closed() {
                if let Err(event) = tx.send(ChannelEvent::Closed) {
                    warn!("Failed to send `{:?}` event.", event);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::oneshot;

    #[test]
    fn it_continues_running_when_receives_nothing() {
        let (mut channel, _tx, _rx) = build_channel();
        let (status, error) = channel.poll();
        assert_eq!(status, EntryStatus::Running);
        assert!(error.is_none());
    }

    #[test]
    fn it_closes_channel_when_receives_closed_event() {
        let (mut channel, tx, mut rx) = build_channel();

        let result = tx.send(ChannelEvent::Closed);
        assert!(result.is_ok());

        let (status, error) = channel.poll();
        assert_eq!(status, EntryStatus::Closed);
        assert!(error.is_none());

        match rx.try_recv() {
            Ok(ChannelEvent::Closed) => {}
            _ => {
                unreachable!("Receiver got unexpected event");
            }
        }
    }

    #[test]
    fn it_closes_channel_when_receives_error_event() {
        let (mut channel, tx, mut rx) = build_channel();

        let result = tx.send(ChannelEvent::Error {
            message: "oops!".to_string(),
            error: anyhow::anyhow!("Something went wrong"),
        });
        assert!(result.is_ok());

        let (status, error) = channel.poll();
        assert_eq!(status, EntryStatus::Error);
        assert_eq!(error, Some("Unexpected error: oops!".to_string()));

        match rx.try_recv() {
            Ok(ChannelEvent::Closed) => {}
            _ => {
                unreachable!("Receiver got unexpected event");
            }
        }
    }

    #[test]
    fn it_closes_channel_when_the_channel_half_is_closed() {
        let (mut channel, tx, mut rx) = build_channel();

        drop(tx);

        let (status, error) = channel.poll();
        assert_eq!(status, EntryStatus::Error);
        assert_eq!(
            error,
            Some("Oneshot channel is closed unexpectedly.".to_string())
        );

        match rx.try_recv() {
            Ok(ChannelEvent::Closed) => {}
            _ => {
                unreachable!("Receiver got unexpected event");
            }
        }
    }

    fn build_channel() -> (Channel, Sender<ChannelEvent>, Receiver<ChannelEvent>) {
        let (tx_0, rx_0) = oneshot::channel::<ChannelEvent>();
        let (tx_1, rx_1) = oneshot::channel::<ChannelEvent>();
        let channel = Channel::new(tx_0, rx_1);
        (channel, tx_1, rx_0)
    }
}
