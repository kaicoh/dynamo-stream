use crate::error::from_guard;

use super::{Client, GetRecordsOutput, GetShardsOutput, Record, Shard};

use anyhow::Result;
use async_trait::async_trait;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct MockClient {
    shard_source: ShardSource,
    record_source: Arc<Mutex<RecordSource>>,
}

#[async_trait]
impl Client for MockClient {
    async fn get_shards(&self, table_name: &str) -> Result<GetShardsOutput> {
        assert_eq!(table_name, self.shard_source.table_name.as_str());

        Ok(GetShardsOutput {
            shards: self.shard_source.shards.clone(),
        })
    }

    async fn get_records(&self, iterator: &str) -> Result<GetRecordsOutput> {
        let mut source = self.record_source.lock().map_err(from_guard)?;

        if let Some(last_iterator) = source.last_iterator.as_deref() {
            assert_eq!(iterator, last_iterator);
        }

        let (next_iterator, records) = match source.next() {
            Some((next_iterator, records)) => {
                source.last_iterator = next_iterator.clone();
                (next_iterator, records)
            }
            None => (None, vec![]),
        };

        Ok(GetRecordsOutput {
            next_iterator,
            records,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ShardSource {
    table_name: String,
    shards: Vec<Shard>,
}

impl ShardSource {
    pub fn new<S, T>(table_name: S, shards: T) -> Self
    where
        S: Into<String>,
        T: IntoIterator<Item = Shard>,
    {
        Self {
            table_name: table_name.into(),
            shards: shards.into_iter().collect(),
        }
    }
}

type IteratorRecords = (Option<String>, Vec<Record>);

#[derive(Debug, Clone)]
pub struct RecordSource {
    last_iterator: Option<String>,
    outputs: Vec<IteratorRecords>,
}

impl RecordSource {
    pub fn new<T>(outputs: T) -> Self
    where
        T: IntoIterator<Item = IteratorRecords>,
    {
        Self {
            last_iterator: None,
            outputs: outputs.into_iter().collect(),
        }
    }
}

impl Iterator for RecordSource {
    type Item = IteratorRecords;

    fn next(&mut self) -> Option<Self::Item> {
        let mut iterator = self.outputs.clone().into_iter();
        let result = iterator.next();

        if let Some((ref next_iterator, _)) = result {
            self.last_iterator = next_iterator.clone();
        }
        self.outputs = iterator.collect();

        result
    }
}
