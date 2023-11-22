use super::{
    event::{ReceiverHalf, TryRecvResult},
    Records,
};

use anyhow::Result;
use axum::async_trait;
use tokio::{
    sync::watch,
    time::{sleep, Duration},
};
use tracing::{error, info};

/// A stream should have one opponent and communicate each other.
#[async_trait]
pub trait Stream: ReceiverHalf + Send + Sync {
    fn table_name(&self) -> &str;

    /// Get records sender.
    fn tx_records(&self) -> &watch::Sender<Records>;

    async fn iterate(&mut self) -> Result<Records>;

    /// You can overwrite this method to implement initialization before iterating.
    async fn init(&mut self) -> Result<()> {
        Ok(())
    }

    /// Start streaming.
    async fn start_streaming(&mut self, interval: Option<u64>) {
        if let Err(err) = self.init().await {
            error!("Failed to initialize stream. Skip starting streaming: {err}");
            error!("{:#?}", err);
            return;
        }

        loop {
            match self.iterate().await {
                Ok(records) => {
                    if self.tx_records().send(records).is_err() {
                        info!(
                            "All record receivers are gone. Stop streaming from \"{}\" table.",
                            self.table_name()
                        );
                        return;
                    }
                }
                Err(err) => {
                    error!(
                        "Failed to iterate. Stop streaming from \"{}\" table: {err}",
                        self.table_name()
                    );
                    return;
                }
            }

            match self.try_recv_event() {
                TryRecvResult::Empty => {}
                TryRecvResult::Received(_) => {
                    info!(
                        "Received an event to stop streaming. Stop streaming from \"{}\" table.",
                        self.table_name()
                    );
                    return;
                }
                TryRecvResult::Error(err) => {
                    error!(
                        "Failed to receive events. Stop streaming from \"{}\" table: {err}",
                        self.table_name()
                    );
                    return;
                }
            }

            if let Some(interval) = interval {
                sleep(Duration::from_secs(interval)).await;
            }
        }
    }
}
