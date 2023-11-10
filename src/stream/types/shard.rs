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

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[cfg(test)]
    pub fn has<S, T>(&self, key: S, value: T) -> bool
    where
        S: Into<String>,
        T: Into<String>,
    {
        let value: String = value.into();
        match self.0.get(&key.into()) {
            Some(iterator_id) => value.as_str() == iterator_id.as_str(),
            None => false,
        }
    }
}

impl Default for Shards {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
impl<I, S, T> From<I> for Shards
where
    I: IntoIterator<Item = (S, T)>,
    S: Into<String>,
    T: Into<String>,
{
    fn from(value: I) -> Self {
        let mut shards = Shards::new();
        for (shard_id, iterator_id) in value.into_iter() {
            shards.insert(shard_id, iterator_id);
        }
        shards
    }
}
