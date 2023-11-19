use super::{into_chrono, into_item, AttributeValue, StreamViewType};

use aws_sdk_dynamodbstreams::types;
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::{
    cmp::{Ord, Ordering, PartialOrd},
    collections::HashMap,
};

#[derive(Debug, Serialize, Clone, Eq, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub struct StreamRecord {
    approximate_creation_date_time: Option<DateTime<Utc>>,
    keys: Option<HashMap<String, AttributeValue>>,
    new_image: Option<HashMap<String, AttributeValue>>,
    old_image: Option<HashMap<String, AttributeValue>>,
    sequence_number: Option<String>,
    size_bytes: Option<i64>,
    stream_view_type: Option<StreamViewType>,
}

impl From<types::StreamRecord> for StreamRecord {
    fn from(value: types::StreamRecord) -> StreamRecord {
        StreamRecord {
            approximate_creation_date_time: value.approximate_creation_date_time.map(into_chrono),
            keys: value.keys.map(into_item),
            new_image: value.new_image.map(into_item),
            old_image: value.old_image.map(into_item),
            sequence_number: value.sequence_number,
            size_bytes: value.size_bytes,
            stream_view_type: value.stream_view_type.map(StreamViewType::from),
        }
    }
}

impl PartialOrd for StreamRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for StreamRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        match (
            self.sequence_number.as_ref(),
            other.sequence_number.as_ref(),
        ) {
            (Some(n), Some(m)) => n.as_str().cmp(m.as_str()),
            (None, Some(_)) => Ordering::Greater,
            (Some(_), None) => Ordering::Less,
            _ => Ordering::Equal,
        }
    }
}
