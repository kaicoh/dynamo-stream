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
        let (tx1, rx1) = oneshot::channel::<Event>();

        let listener = Listener {
            url,
            tx_event: Some(tx0),
            rx_event: rx1,
            rx_records: rx,
        };

        let half = ListenerHalf {
            tx_event: Some(tx1),
            rx_event: rx0,
        };

        (listener, half)
    }
}

#[derive(Debug)]
pub struct ListenerHalf {
    tx_event: Option<oneshot::Sender<Event>>,
    rx_event: oneshot::Receiver<Event>,
}

impl HandleEvent for ListenerHalf {
    fn tx_event(&mut self) -> Option<oneshot::Sender<Event>> {
        self.tx_event.take()
    }

    fn rx_event(&mut self) -> &mut oneshot::Receiver<Event> {
        &mut self.rx_event
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
