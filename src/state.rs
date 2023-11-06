use crate::types::{ClonableEntry, Entry};

use std::collections::{hash_map::IterMut, HashMap};

#[derive(Debug)]
pub struct AppState(HashMap<String, Entry>);

impl AppState {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn insert<T: Into<String>>(&mut self, id: T, entry: Entry) -> Option<Entry> {
        self.0.insert(id.into(), entry)
    }

    pub fn set_removed<T: Into<String>>(&mut self, id: T) -> Option<()> {
        self.0.get_mut(&id.into()).map(Entry::mark_removed)
    }

    pub fn remove<T: Into<String>>(&mut self, id: T) -> Option<Entry> {
        self.0.remove(&id.into())
    }

    pub fn iter_mut(&mut self) -> IterMut<'_, String, Entry> {
        self.0.iter_mut()
    }

    pub fn serialize(&self) -> HashMap<String, ClonableEntry> {
        let mut map: HashMap<String, ClonableEntry> = HashMap::new();

        for (id, e) in self.0.iter() {
            let entry = ClonableEntry::new(e);
            map.insert(id.into(), entry);
        }

        map
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}
