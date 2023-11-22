mod attribute_value;
mod identity;
mod operation_type;
mod record;
mod records;
mod shard_iterator_type;
mod stream_record;
mod stream_status;
mod stream_view_type;

pub use attribute_value::AttributeValue;
pub use identity::Identity;
pub use operation_type::OperationType;
pub use record::Record;
pub use records::Records;
pub use shard_iterator_type::ShardIteratorType;
pub use stream_record::StreamRecord;
pub use stream_status::StreamStatus;
pub use stream_view_type::StreamViewType;

use aws_sdk_dynamodbstreams::{primitives, types};
use chrono::{DateTime, Utc};
use std::collections::HashMap;

fn into_str(blob: primitives::Blob) -> String {
    String::from_utf8_lossy(&blob.into_inner()).into_owned()
}

fn into_chrono(datetime: primitives::DateTime) -> DateTime<Utc> {
    let secs = datetime.secs();
    let nsecs = datetime.subsec_nanos();
    DateTime::<Utc>::from_timestamp(secs, nsecs).expect("Invalid timestamp")
}

fn into_item(value: HashMap<String, types::AttributeValue>) -> HashMap<String, AttributeValue> {
    let mut map: HashMap<String, AttributeValue> = HashMap::new();
    for (key, val) in value.iter() {
        map.insert(key.to_owned(), AttributeValue::from(val.clone()));
    }
    map
}
