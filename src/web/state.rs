use super::{
    subscription::{Destination, Subscription},
    DynamodbClient,
};

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

pub type SharedState = Arc<Mutex<AppState>>;

impl From<AppState> for SharedState {
    fn from(state: AppState) -> Self {
        Arc::new(Mutex::new(state))
    }
}

#[derive(Debug)]
pub struct AppState {
    client: DynamodbClient,
    subscriptions: Vec<Subscription>,
}

impl AppState {
    pub fn new(client: DynamodbClient) -> Self {
        Self {
            client,
            subscriptions: vec![],
        }
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

    pub fn add_sub(&mut self, sub: Subscription) {
        self.subscriptions.push(sub);
    }

    pub fn add_listener(&mut self, table: String, url: String) -> Option<Destination> {
        self.sub(&table)
            .as_mut()
            .map(|sub| Destination::from(sub.set_listener(url)))
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
