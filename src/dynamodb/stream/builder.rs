use super::*;

#[derive(Default)]
pub struct DynamodbStreamBuilder {
    client: Option<Arc<dyn Client>>,
    table: Option<String>,
}

impl DynamodbStreamBuilder {
    pub fn new() -> Self {
        Self {
            client: None,
            table: None,
        }
    }

    pub fn set_client(self, client: Arc<dyn Client>) -> Self {
        Self {
            client: Some(client),
            ..self
        }
    }

    pub fn set_table<T: Into<String>>(self, table: T) -> Self {
        Self {
            table: Some(table.into()),
            ..self
        }
    }

    pub async fn build(self) -> Result<(DynamodbStream, DynamodbStreamHalf)> {
        let client = self.client.ok_or(not_set_field("client"))?;
        let table = self.table.ok_or(not_set_field("table"))?;

        let arn = client.get_stream_arn(&table).await?.stream_arn;

        let shards = get_all_shards(Arc::clone(&client), &arn).await?;
        let shards = set_shard_iterators(Arc::clone(&client), &arn, shards).await;

        let (tx0, rx0) = oneshot::channel::<Event>();
        let (tx1, rx1) = oneshot::channel::<Event>();
        let (tx2, rx2) = watch::channel(Records::new());

        let stream = DynamodbStream {
            client,
            arn,
            table,
            tx_event: Some(tx0),
            rx_event: rx1,
            tx_records: tx2,
            shards,
        };

        let half = DynamodbStreamHalf {
            tx_event: Some(tx1),
            rx_event: rx0,
            rx_records: rx2,
        };

        Ok((stream, half))
    }
}

fn not_set_field(field: &str) -> anyhow::Error {
    anyhow::anyhow!("\"{field}\" is not set to DynamodbStreamBuilder")
}

#[derive(Debug)]
pub struct DynamodbStreamHalf {
    tx_event: Option<oneshot::Sender<Event>>,
    rx_event: oneshot::Receiver<Event>,
    rx_records: watch::Receiver<Records>,
}

impl DynamodbStreamHalf {
    pub fn receiver(&self) -> watch::Receiver<Records> {
        self.rx_records.clone()
    }
}

impl HandleEvent for DynamodbStreamHalf {
    fn tx_event(&mut self) -> Option<oneshot::Sender<Event>> {
        self.tx_event.take()
    }

    fn rx_event(&mut self) -> &mut oneshot::Receiver<Event> {
        &mut self.rx_event
    }
}

impl Drop for DynamodbStreamHalf {
    // Close channel
    fn drop(&mut self) {
        if let Some(tx) = self.tx_event().take() {
            let _ = tx.send(Event::Close);
        }
    }
}
