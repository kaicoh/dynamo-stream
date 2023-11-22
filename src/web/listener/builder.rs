use super::*;

#[derive(Debug, Default)]
pub struct ListenerBuilder {
    url: Option<String>,
    rx: Option<watch::Receiver<Records>>,
}

impl ListenerBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_url<T: Into<String>>(self, url: T) -> Self {
        Self {
            url: Some(url.into()),
            ..self
        }
    }

    pub fn set_records_receiver(self, rx: watch::Receiver<Records>) -> Self {
        Self {
            rx: Some(rx),
            ..self
        }
    }

    pub fn build(self) -> (Listener, ListenerHalf) {
        let url = self.url.expect("\"url\" is not set to ListenerBuilder");
        let rx = self
            .rx
            .expect("\"rx_records\" is not set to ListenerBuilder");

        let (tx0, rx0) = oneshot::channel::<Event>();

        let listener = Listener {
            url,
            rx_event: rx0,
            rx_records: rx,
        };

        let half = ListenerHalf {
            tx_event: Some(tx0),
        };

        (listener, half)
    }
}

#[derive(Debug)]
pub struct ListenerHalf {
    tx_event: Option<oneshot::Sender<Event>>,
}

impl SenderHalf for ListenerHalf {
    fn tx_event(&mut self) -> Option<oneshot::Sender<Event>> {
        self.tx_event.take()
    }
}

impl Drop for ListenerHalf {
    // Close channel
    fn drop(&mut self) {
        if let Some(tx) = self.tx_event().take() {
            let _ = tx.send(Event::Close);
        }
    }
}
