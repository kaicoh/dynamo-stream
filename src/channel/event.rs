use tokio::sync::oneshot::{error::TryRecvError, Receiver, Sender};
use tracing::error;

#[derive(Debug)]
pub enum Event {
    Close,
}

#[derive(Debug)]
pub enum TryRecvResult {
    Received(Event),
    Empty,
    Error(anyhow::Error),
}

pub trait ReceiverHalf {
    /// Get event receiver.
    fn rx_event(&mut self) -> &mut Receiver<Event>;

    /// Recieve event and return result with wrapper enum.
    fn try_recv_event(&mut self) -> TryRecvResult {
        match self.rx_event().try_recv() {
            Ok(event) => TryRecvResult::Received(event),
            Err(TryRecvError::Empty) => TryRecvResult::Empty,
            Err(err) => TryRecvResult::Error(anyhow::Error::new(err)),
        }
    }
}

pub trait SenderHalf {
    /// Get event sender. If the result is None, the sender is already consumes by `send` method.
    fn tx_event(&mut self) -> Option<Sender<Event>>;

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
}
