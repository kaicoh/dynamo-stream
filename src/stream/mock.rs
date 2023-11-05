use super::super::Result;
use super::client::{DescribeStreamResult, GetRecordsResult, GetShardIteratorResult, StreamClient};
use super::Record;

use async_trait::async_trait;
use aws_sdk_dynamodbstreams::types::ShardIteratorType;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct ShardClient {
    stream_arn: String,
    source: Arc<Mutex<ShardSource>>,
}

impl ShardClient {
    pub fn new(stream_arn: &str, source: ShardSource) -> Self {
        Self {
            stream_arn: stream_arn.into(),
            source: Arc::new(Mutex::new(source)),
        }
    }
}

#[async_trait]
impl StreamClient for ShardClient {
    async fn describe_stream(
        &self,
        stream_arn: &str,
        exclusive_id: Option<String>,
    ) -> Result<Option<DescribeStreamResult>> {
        assert_eq!(self.stream_arn.as_str(), stream_arn);

        let mut source = self.source.lock().unwrap();
        assert_eq!(source.last_evaluated_shard_id, exclusive_id);

        Ok(Some(source.describe_stream()))
    }

    async fn get_shard_iterator(
        &self,
        stream_arn: &str,
        shard_id: &str,
        shard_iterator_type: ShardIteratorType,
    ) -> Result<GetShardIteratorResult> {
        assert_eq!(self.stream_arn.as_str(), stream_arn);
        assert_eq!(shard_iterator_type, ShardIteratorType::Latest);

        let source = self.source.lock().unwrap();
        Ok(source.get_shard_iterator(shard_id))
    }

    async fn get_records(&self, shard_iterator: Option<String>) -> Result<GetRecordsResult> {
        // MEMO:
        // If set to None, the shard has been closed and the requested iterator will not return any more data.
        assert!(shard_iterator.is_some());

        let mut source = self.source.lock().unwrap();
        let res = shard_iterator
            .as_deref()
            .map(|iterator_id| source.get_records(iterator_id))
            .unwrap_or_default();
        Ok(res)
    }
}

#[derive(Debug, Clone)]
pub struct ShardSource {
    cursor: usize,
    chunk_size: usize,
    last_evaluated_shard_id: Option<String>,
    shards: Vec<Shard>,
}

impl ShardSource {
    pub fn new<T: IntoIterator<Item = Shard>>(shards: T) -> Self {
        Self {
            cursor: 0,
            chunk_size: 2,
            last_evaluated_shard_id: None,
            shards: shards.into_iter().collect(),
        }
    }

    pub fn describe_stream(&mut self) -> DescribeStreamResult {
        let shard_ids = self.next().unwrap_or_default().into_iter().map(|s| s.id);

        let last_evaluated_shard_id = if self.cursor * self.chunk_size < self.shards.len() {
            shard_ids.clone().last()
        } else {
            None
        };

        self.last_evaluated_shard_id = last_evaluated_shard_id.clone();

        DescribeStreamResult {
            shard_ids: Box::new(shard_ids),
            last_evaluated_shard_id,
        }
    }

    /// Return iterator id which is not empty.
    pub fn get_shard_iterator(&self, shard_id: &str) -> GetShardIteratorResult {
        let shard_iterator = self
            .shards
            .iter()
            .find(|&s| s.id.as_str() == shard_id)
            .and_then(|s| s.get_iterator_id());

        GetShardIteratorResult { shard_iterator }
    }

    pub fn get_records(&mut self, iterator_id: &str) -> GetRecordsResult {
        self.shards
            .iter_mut()
            .find(|s| s.has_iterator(iterator_id))
            .map(|shard| {
                let records = shard.get_next(iterator_id).unwrap_or_default();
                let next_shard_iterator = shard.get_iterator_id().unwrap_or("no-records".into());

                GetRecordsResult {
                    next_shard_iterator: Some(next_shard_iterator),
                    records,
                }
            })
            .unwrap_or_default()
    }
}

impl Iterator for ShardSource {
    type Item = Vec<Shard>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut chunks = self
            .shards
            .as_slice()
            .chunks(self.chunk_size)
            .skip(self.cursor);

        self.cursor = self.cursor + 1;

        chunks.next().map(|v| v.to_vec())
    }
}

#[derive(Debug, Clone)]
pub struct Shard {
    id: String,
    // key is shard iterator id, value is shard iterator.
    map: BTreeMap<String, ShardIterator>,
}

impl Shard {
    pub fn new<T: Into<String>>(id: T) -> Self {
        Self {
            id: id.into(),
            map: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, key: &str, value: ShardIterator) {
        self.map.insert(key.into(), value);
    }

    /// Return iterator id which is not empty.
    pub fn get_iterator_id(&self) -> Option<String> {
        self.map
            .iter()
            .filter(|(_, i)| !i.is_empty())
            .map(|(id, _)| id.to_string())
            .next()
    }

    pub fn has_iterator(&self, iterator_id: &str) -> bool {
        self.map.iter().any(|(id, _)| id.as_str() == iterator_id)
    }

    pub fn get_next(&mut self, iterator_id: &str) -> Option<Vec<Record>> {
        if let Some(iterator) = self.map.get(iterator_id) {
            let mut iterator = iterator.clone();
            let records = iterator.next();
            self.insert(iterator_id, iterator);
            records
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub struct ShardIterator {
    chunk_size: usize,
    records: Vec<Record>,
}

impl ShardIterator {
    pub fn new<S, T>(ids: S) -> Self
    where
        S: IntoIterator<Item = T>,
        T: Into<String>,
    {
        Self {
            chunk_size: 3,
            records: ids.into_iter().map(Record::new).collect(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

impl Iterator for ShardIterator {
    type Item = Vec<Record>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut chunks = self.records.as_slice().chunks(self.chunk_size);

        let chunk: Option<Vec<Record>> = chunks.next().map(|v| v.to_vec());

        self.records = chunks
            .collect::<Vec<&[Record]>>()
            .into_iter()
            .map(|v| v.to_vec())
            .flatten()
            .collect();

        chunk
    }
}

#[test]
fn test_shard_iterator() {
    let mut iter = ShardIterator::new(["foo", "bar", "baz", "foobar"]);
    let opt = iter.next();
    assert!(opt.is_some());

    let items = opt.unwrap();
    assert_eq!(items.len(), 3);

    let opt = iter.next();
    assert!(opt.is_some());

    let items = opt.unwrap();
    assert_eq!(items.len(), 1);

    let opt = iter.next();
    assert!(opt.is_none());
}

#[test]
fn test_shard() {
    let mut shard = Shard::new("test shard");
    let iter_0 = ShardIterator::new(["a", "b", "c", "d", "e"]);
    let iter_1 = ShardIterator::new(["f", "g", "h", "i"]);
    shard.insert("iter_0", iter_0);
    shard.insert("iter_1", iter_1);

    // Get next iterator id which is not empty
    let iterator_id = shard.get_iterator_id();
    assert!(iterator_id.is_some());
    assert_eq!(iterator_id.unwrap(), "iter_0");

    let opt = shard.get_next("iter_0");
    assert!(opt.is_some());

    let items = opt.unwrap();
    assert_eq!(items.len(), 3);
    assert_eq!(record_event_ids(items), ["a", "b", "c"]);

    // Next iterator id is still iter_0 since iter_0 is not empty.
    let iterator_id = shard.get_iterator_id();
    assert!(iterator_id.is_some());
    assert_eq!(iterator_id.unwrap(), "iter_0");

    let opt = shard.get_next("iter_0");
    assert!(opt.is_some());

    let items = opt.unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(record_event_ids(items), ["d", "e"]);

    // Now the next iterator id is iter_1
    let iterator_id = shard.get_iterator_id();
    assert!(iterator_id.is_some());
    assert_eq!(iterator_id.unwrap(), "iter_1");

    // Confirm iter_0 is empty
    let opt = shard.get_next("iter_0");
    assert!(opt.is_none());

    let opt = shard.get_next("iter_1");
    assert!(opt.is_some());

    let items = opt.unwrap();
    assert_eq!(items.len(), 3);
    assert_eq!(record_event_ids(items), ["f", "g", "h"]);

    // Next iterator id is still iter_1 since iter_1 is not empty.
    let iterator_id = shard.get_iterator_id();
    assert!(iterator_id.is_some());
    assert_eq!(iterator_id.unwrap(), "iter_1");

    let opt = shard.get_next("iter_1");
    assert!(opt.is_some());

    let items = opt.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(record_event_ids(items), ["i"]);

    // Now all iterators are consumes and next iterator id is None.
    let iterator_id = shard.get_iterator_id();
    assert!(iterator_id.is_none());

    // Confirm iter_1 is empty
    let opt = shard.get_next("iter_1");
    assert!(opt.is_none());
}

#[test]
fn test_shard_source_get_shard_iterator() {
    let source = shard_source_factory();

    let GetShardIteratorResult { shard_iterator } = source.get_shard_iterator("shard_0");
    assert!(shard_iterator.is_some());
    assert_eq!(shard_iterator.unwrap(), "iter_00");

    let GetShardIteratorResult { shard_iterator } = source.get_shard_iterator("shard_1");
    assert!(shard_iterator.is_some());
    assert_eq!(shard_iterator.unwrap(), "iter_10");

    let GetShardIteratorResult { shard_iterator } = source.get_shard_iterator("shard_2");
    assert!(shard_iterator.is_none());
}

#[test]
fn test_shard_source_describe_stream() {
    let mut source = shard_source_factory();

    let DescribeStreamResult {
        shard_ids,
        last_evaluated_shard_id,
    } = source.describe_stream();
    assert_eq!(
        shard_ids.into_iter().collect::<Vec<String>>(),
        ["shard_0", "shard_1"]
    );
    assert!(last_evaluated_shard_id.is_some());
    assert_eq!(last_evaluated_shard_id.unwrap(), "shard_1");

    let DescribeStreamResult {
        shard_ids,
        last_evaluated_shard_id,
    } = source.describe_stream();
    assert_eq!(shard_ids.into_iter().collect::<Vec<String>>(), ["shard_2"]);
    assert!(last_evaluated_shard_id.is_none());
}

#[test]
fn test_shard_source_get_records() {
    let mut source = shard_source_factory();

    let GetRecordsResult {
        next_shard_iterator,
        records,
    } = source.get_records("iter_00");
    assert!(next_shard_iterator.is_some());
    assert_eq!(next_shard_iterator.unwrap(), "no-records");
    assert_eq!(records.len(), 1);
    assert_eq!(record_event_ids(records), ["a"]);

    let GetRecordsResult {
        next_shard_iterator,
        records,
    } = source.get_records("iter_10");
    assert!(next_shard_iterator.is_some());
    assert_eq!(next_shard_iterator.unwrap(), "iter_10");
    assert_eq!(records.len(), 3);
    assert_eq!(record_event_ids(records), ["b", "c", "d"]);

    let GetRecordsResult {
        next_shard_iterator,
        records,
    } = source.get_records("iter_10");
    assert!(next_shard_iterator.is_some());
    assert_eq!(next_shard_iterator.unwrap(), "iter_11");
    assert_eq!(records.len(), 1);
    assert_eq!(record_event_ids(records), ["e"]);

    let GetRecordsResult {
        next_shard_iterator,
        records,
    } = source.get_records("iter_11");
    assert!(next_shard_iterator.is_some());
    assert_eq!(next_shard_iterator.unwrap(), "no-records");
    assert_eq!(records.len(), 2);
    assert_eq!(record_event_ids(records), ["f", "g"]);

    let GetRecordsResult {
        next_shard_iterator,
        records,
    } = source.get_records("iter_20");
    assert!(next_shard_iterator.is_some());
    assert_eq!(next_shard_iterator.unwrap(), "no-records");
    assert!(records.is_empty());
}

pub fn shard_source_factory() -> ShardSource {
    let mut shard_0 = Shard::new("shard_0");
    let iter_00 = ShardIterator::new(["a"]);
    shard_0.insert("iter_00", iter_00);

    let mut shard_1 = Shard::new("shard_1");
    let iter_10 = ShardIterator::new(["b", "c", "d", "e"]);
    let iter_11 = ShardIterator::new(["f", "g"]);
    shard_1.insert("iter_10", iter_10);
    shard_1.insert("iter_11", iter_11);

    let mut shard_2 = Shard::new("shard_2");
    let iter_20 = ShardIterator::new(Vec::<String>::new());
    shard_2.insert("iter_20", iter_20);

    ShardSource::new([shard_0, shard_1, shard_2])
}

pub fn record_event_ids(records: Vec<Record>) -> Vec<String> {
    records.into_iter().filter_map(|r| r.event_id()).collect()
}
