mod builder;

use super::{
    listener::{Listener, ListenerHalf},
    Consumer, DynamodbStreamHalf,
};

use serde::Serialize;
use std::collections::HashMap;
use ulid::Ulid;

pub use builder::SubscriptionBuilder;

#[derive(Debug)]
pub struct Subscription {
    table: String,
    destinations: HashMap<String, String>,
    stream_half: DynamodbStreamHalf,
    listener_halfs: HashMap<String, ListenerHalf>,
}

impl Subscription {
    pub fn builder() -> SubscriptionBuilder {
        SubscriptionBuilder::new()
    }

    pub fn table(&self) -> &str {
        self.table.as_str()
    }

    pub fn serialize(&self) -> (String, Vec<Destination>) {
        (
            self.table.clone(),
            Destination::from_hashmap(self.destinations.clone()),
        )
    }

    pub fn set_listener<T: Into<String>>(&mut self, url: T) -> (String, String) {
        let url: String = url.into();
        let id = Ulid::new().to_string();

        if self.has_dest(&id) {
            self.add_dest(&id, &url);
        }

        if self.has_listener(&id) {
            self.add_listener(&id, &url);
        }

        (id, url)
    }

    pub fn unset_listener<T: Into<String>>(&mut self, id: T) {
        let id: String = id.into();
        self.remove_dest(&id);
        self.remove_listener(&id);
    }

    fn has_dest(&self, id: &str) -> bool {
        self.destinations.contains_key(id)
    }

    fn add_dest(&mut self, id: &str, url: &str) {
        self.destinations.insert(id.into(), url.into());
    }

    fn remove_dest(&mut self, id: &str) {
        self.destinations.remove(id);
    }

    fn has_listener(&self, id: &str) -> bool {
        self.listener_halfs.contains_key(id)
    }

    fn add_listener(&mut self, id: &str, url: &str) {
        let receiver = self.stream_half.receiver();
        let (mut listener, listener_half) = Listener::builder()
            .set_url(url)
            .set_records_receiver(receiver)
            .build();

        tokio::spawn(async move {
            listener.start_consuming().await;
        });

        self.listener_halfs.insert(id.into(), listener_half);
    }

    fn remove_listener(&mut self, id: &str) {
        // When the listener_half drops, the associated listener will also
        // drop due to the listener_half's drop trait.
        self.listener_halfs.remove(id);
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Destination {
    id: String,
    url: String,
}

impl From<(String, String)> for Destination {
    fn from((id, url): (String, String)) -> Self {
        Self { id, url }
    }
}

impl Destination {
    fn from_hashmap(map: HashMap<String, String>) -> Vec<Self> {
        map.into_iter().map(Destination::from).collect()
    }
}
