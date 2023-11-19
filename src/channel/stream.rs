use super::{event::Event, Records};

use anyhow::Result;
use axum::async_trait;
use tokio::{
    sync::{oneshot::{self, error::TryRecvError}, watch},
    time::{sleep, Duration},
};
use tracing::{error, info};

/// A stream should have one opponent and communicate each other.
#[async_trait]
pub trait Stream: Send + Sync {
    fn table_name(&self) -> &str;

    /// Get event sender. If the result is None, the sender is already consumes by `send` method.
    fn tx_event(&mut self) -> Option<oneshot::Sender<Event>>;

    /// Get event receiver.
    fn rx_event(&mut self) -> &mut oneshot::Receiver<Event>;

    /// Get records sender.
    fn tx_records(&self) -> &watch::Sender<Records>;

    async fn iterate(&mut self) -> Result<Records>;

    /// Send event to the opponent. Calling this method means stopping stream because the sender is
    /// an oneshot sender.
    fn send_event(&mut self, event: Event) {
        if let Err(err) = self
            .tx_event()
            .expect("Stream doesn't have oneshot event sender.")
            .send(event)
        {
            error!("{:#?}", err);
        }
    }

    /// Start streaming.
    async fn start(&mut self, interval: Option<u64>) {
        loop {
            match self.iterate().await {
                Ok(records) => {
                    if let Err(_) = self.tx_records().send(records) {
                        info!("All record receivers are gone. Stop streaming from \"{}\" table.", self.table_name());
                        return;
                    }
                },
                Err(err) => {
                    error!("Failed to iterate. Stop streaming from \"{}\" table.", self.table_name());
                    self.send_event(Event::Error(err));
                    return;
                },
            }

            match self.rx_event().try_recv() {
                Err(TryRecvError::Empty) => {},
                Ok(_) => {
                    info!("Received an event to stop streaming. Stop streaming from \"{}\" table.", self.table_name());
                    return;
                }
                Err(err) => {
                    error!("Failed to receive events. Stop streaming from \"{}\" table.", self.table_name());
                    self.send_event(Event::Error(anyhow::Error::new(err)));
                    return;
                }
            }

            if let Some(interval) = interval {
                sleep(Duration::from_secs(interval)).await;
            }
        }
    }
}
