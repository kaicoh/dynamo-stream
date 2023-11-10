mod channel;
mod entry;
mod subscription;

use crate::stream::notification::Event as NotiEvent;

use super::EntryConfig;

use channel::Channel;
pub use entry::{Entry, EntryState, EntryStatus};
use subscription::Subscription;

use std::collections::{hash_map::IterMut, HashMap};
use std::fmt;
use tokio::sync::mpsc::{error::SendError, Sender};
use ulid::Ulid;

#[derive(Debug)]
pub enum ChannelEvent {
    Closed,
    Error {
        message: String,
        error: anyhow::Error,
    },
}

impl fmt::Display for ChannelEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ChannelEvent::Closed => write!(f, "CLOSED"),
            ChannelEvent::Error { .. } => write!(f, "ERROR"),
        }
    }
}

impl From<SendError<NotiEvent>> for ChannelEvent {
    fn from(error: SendError<NotiEvent>) -> ChannelEvent {
        let message = format!("Failed to send notification event: {error}");
        let error = anyhow::Error::from(error);
        ChannelEvent::Error { message, error }
    }
}

#[derive(Debug)]
pub struct AppState {
    notifier: Sender<NotiEvent>,
    entries: HashMap<String, Entry>,
}

impl AppState {
    pub fn new(notifier: Sender<NotiEvent>, configs: Vec<EntryConfig>) -> Self {
        let mut entries: HashMap<String, Entry> = HashMap::new();

        for config in configs {
            let id = Ulid::new().to_string();
            let notifier = notifier.clone();
            let entry = Entry::from_conf(config, notifier);
            entries.insert(id, entry);
        }

        Self { notifier, entries }
    }

    pub fn get_notifier(&self) -> Sender<NotiEvent> {
        self.notifier.clone()
    }

    pub fn insert<T: Into<String>>(&mut self, id: T, entry: Entry) -> Option<Entry> {
        self.entries.insert(id.into(), entry)
    }

    pub fn set_removed<T: Into<String>>(&mut self, id: T) -> Option<()> {
        self.entries.get_mut(&id.into()).map(Entry::set_removed)
    }

    pub fn remove<T: Into<String>>(&mut self, id: T) -> Option<Entry> {
        self.entries.remove(&id.into())
    }

    pub fn iter_mut(&mut self) -> IterMut<'_, String, Entry> {
        self.entries.iter_mut()
    }

    pub fn entry_states(&self) -> HashMap<String, EntryState> {
        let mut states: HashMap<String, EntryState> = HashMap::new();

        for (id, e) in self.entries.iter() {
            states.insert(id.into(), EntryState::from(e));
        }

        states
    }
}
