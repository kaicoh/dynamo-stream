use super::{Client, GetRecordsOutput, GetShardsOutput, Records, Shards};

use anyhow::Result;
use axum::async_trait;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct SourceClient {
    shards_source: Arc<Mutex<ShardsSource>>,
    records_source: Arc<Mutex<RecordsSource>>,
}

impl SourceClient {
    pub fn new(
        shards_source: Arc<Mutex<ShardsSource>>,
        records_source: Arc<Mutex<RecordsSource>>,
    ) -> Self {
        Self {
            shards_source,
            records_source,
        }
    }
}

#[async_trait]
impl Client for SourceClient {
    async fn get_shards(&self, table_name: &str) -> Result<GetShardsOutput> {
        let mut source = self.shards_source.lock().unwrap();
        assert_eq!(source.table_name.as_str(), table_name);

        Ok(GetShardsOutput {
            shards: source.next().unwrap_or_default(),
        })
    }

    async fn get_records(&self, _shards: &Shards) -> Result<GetRecordsOutput> {
        let mut source = self.shards_source.lock().unwrap();
        let shards = source.next().unwrap_or_default();

        let mut source = self.records_source.lock().unwrap();
        let records = source.next().unwrap_or_default();

        Ok(GetRecordsOutput { shards, records })
    }
}

#[derive(Debug)]
pub struct ShardsSource {
    table_name: String,
    source: Option<Vec<Shards>>,
}

impl Iterator for ShardsSource {
    type Item = Shards;

    fn next(&mut self) -> Option<Self::Item> {
        let mut source = self.source.take().unwrap().into_iter();
        let item = source.next();
        self.source = Some(source.collect());
        item
    }
}

impl ShardsSource {
    pub fn new(table_name: &str) -> Self {
        Self {
            table_name: table_name.into(),
            source: Some(vec![]),
        }
    }

    pub fn push<T: Into<Shards>>(&mut self, shards: T) {
        match self.source.as_mut() {
            Some(source) => {
                source.push(shards.into());
            }
            None => {
                self.source = Some(vec![shards.into()]);
            }
        }
    }
}

#[test]
fn shards_source_implements_iterator_trait() {
    let mut source = ShardsSource::new("table");
    source.push([("shard_0", "iterator_0"), ("shard_1", "iterator_1")]);
    source.push([("shard_2", "iterator_2")]);

    let opt = source.next();
    assert!(opt.is_some());
    let shards = opt.unwrap();
    assert_eq!(shards.len(), 2);
    assert!(shards.has("shard_0", "iterator_0"));
    assert!(shards.has("shard_1", "iterator_1"));

    let opt = source.next();
    assert!(opt.is_some());
    let shards = opt.unwrap();
    assert_eq!(shards.len(), 1);
    assert!(shards.has("shard_2", "iterator_2"));

    let opt = source.next();
    assert!(opt.is_none());
}

#[derive(Debug, Default)]
pub struct RecordsSource {
    source: Option<Vec<Records>>,
}

impl Iterator for RecordsSource {
    type Item = Records;

    fn next(&mut self) -> Option<Self::Item> {
        let mut source = self.source.take().unwrap().into_iter();
        let item = source.next();
        self.source = Some(source.collect());
        item
    }
}

impl RecordsSource {
    pub fn new() -> Self {
        Self {
            source: Some(vec![]),
        }
    }

    pub fn push<T: Into<Records>>(&mut self, records: T) {
        match self.source.as_mut() {
            Some(source) => {
                source.push(records.into());
            }
            None => {
                self.source = Some(vec![records.into()]);
            }
        }
    }
}

#[test]
fn records_source_implements_iterator_trait() {
    let mut source = RecordsSource::new();
    source.push(["record_0", "record_1"]);
    source.push(["record_2"]);

    let opt = source.next();
    assert!(opt.is_some());
    let records = opt.unwrap();
    assert_eq!(records.len(), 2);
    assert!(records.includes("record_0"));
    assert!(records.includes("record_1"));

    let opt = source.next();
    assert!(opt.is_some());
    let records = opt.unwrap();
    assert_eq!(records.len(), 1);
    assert!(records.includes("record_2"));

    let opt = source.next();
    assert!(opt.is_none());
}
