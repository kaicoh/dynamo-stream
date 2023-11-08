use std::collections::{hash_map::Iter, HashMap};

#[derive(Debug, Clone)]
pub struct Shards(HashMap<String, String>);

impl Shards {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn insert<S, T>(&mut self, id: S, iterator: T) -> Option<String>
    where
        S: Into<String>,
        T: Into<String>,
    {
        self.0.insert(id.into(), iterator.into())
    }

    pub fn remove(&mut self, id: &str) -> Option<String> {
        self.0.remove(id)
    }

    pub fn iter(&self) -> Iter<'_, String, String> {
        self.0.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Default for Shards {
    fn default() -> Self {
        Self::new()
    }
}
