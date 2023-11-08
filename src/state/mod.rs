mod channel;
mod entry;
mod subscription;

use crate::notification::Event as NotiEvent;

use channel::Channel;
pub use entry::{Entry, EntryState, EntryStatus};
use subscription::Subscription;

use std::collections::{hash_map::IterMut, HashMap};
use std::fmt;
use tokio::sync::mpsc::Sender;

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

impl ChannelEvent {
    fn new_err<T: Into<String>>(error: anyhow::Error, message: T) -> Self {
        Self::Error {
            message: message.into(),
            error,
        }
    }
}

#[derive(Debug)]
pub struct AppState {
    notifier: Sender<NotiEvent>,
    entries: HashMap<String, Entry>,
}

impl AppState {
    pub fn new(notifier: Sender<NotiEvent>) -> Self {
        Self {
            notifier,
            entries: HashMap::new(),
        }
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
