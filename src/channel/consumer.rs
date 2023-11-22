use super::{
    event::{ReceiverHalf, TryRecvResult},
    Records,
};

use axum::async_trait;
use tokio::sync::watch;
use tracing::{error, info, warn};

#[async_trait]
pub trait Consumer: ReceiverHalf + Send + Sync {
    fn identifier(&self) -> &str;

    /// Get records receiver.
    fn rx_records(&mut self) -> &mut watch::Receiver<Records>;

    /// Consume dynamodb stream
    async fn consume(&self, records: Records) -> ();

    /// Start consuming.
    async fn start_consuming(&mut self) {
        loop {
            match self.rx_records().changed().await {
                Ok(_) => {
                    let records = {
                        let records = self.rx_records().borrow_and_update();
                        (*records).clone()
                    };
                    self.consume(records).await;
                }
                Err(err) => {
                    warn!(
                        "Failed to detect watch channel changes. This means the sender of this channel has been dropped. Stop consuming: \"{}\".",
                        self.identifier()
                    );
                    warn!("{:#?}", err);
                    return;
                }
            }

            match self.try_recv_event() {
                TryRecvResult::Empty => {}
                TryRecvResult::Received(_) => {
                    info!(
                        "Received an event to stop consuming: \"{}\".",
                        self.identifier()
                    );
                    return;
                }
                TryRecvResult::Error(err) => {
                    error!(
                        "Failed to receive events. Stop consuming: \"{}\".",
                        self.identifier()
                    );
                    error!("{:#?}", err);
                    return;
                }
            }
        }
    }
}
