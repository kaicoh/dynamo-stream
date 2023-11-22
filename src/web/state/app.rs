use super::{Config, Destination, DynamodbClient, Subscription};

use std::{collections::HashMap, sync::Arc};

#[derive(Debug)]
pub struct AppState {
    client: DynamodbClient,
    subscriptions: Vec<Subscription>,
}

impl AppState {
    pub async fn new(config: &Config) -> Self {
        let client = DynamodbClient::builder()
            .await
            .endpoint_url(config.endpoint_url())
            .build();

        let mut state = Self {
            client,
            subscriptions: vec![],
        };

        for entry in config.entries() {
            state.add_sub(entry.table_name, entry.url);
        }

        state
    }

    pub fn client(&self) -> Arc<DynamodbClient> {
        Arc::new(self.client.clone())
    }

    pub fn serialize(&self) -> HashMap<String, Vec<Destination>> {
        self.subscriptions
            .iter()
            .fold(HashMap::new(), |mut acc, sub| {
                let (table, destinations) = sub.serialize();
                acc.insert(table, destinations);
                acc
            })
    }

    pub fn add_sub(&mut self, table: String, url: String) -> Destination {
        let url = url.as_str();

        self.sub(&table)
            .as_mut()
            .map(|sub| Destination::from(sub.set_listener(url)))
            .unwrap_or_else(|| {
                let client = Arc::new(self.client.clone());

                let mut sub = Subscription::builder()
                    .set_client(client)
                    .set_table(&table)
                    .build();
                let dest = sub.set_listener(url);

                self.subscriptions.push(sub);

                Destination::from(dest)
            })
    }

    pub fn remove_listener(&mut self, table: String, id: String) {
        if let Some(sub) = self.sub(&table).as_mut() {
            sub.unset_listener(id);
        }
    }

    pub fn remove_sub(&mut self, table: String) {
        self.subscriptions.retain(|s| s.table() != table.as_str());
    }

    pub fn has_sub(&mut self, table: &str) -> bool {
        self.sub(table).is_some()
    }

    fn sub(&mut self, table: &str) -> Option<&mut Subscription> {
        self.subscriptions.iter_mut().find(|s| s.table() == table)
    }
}
