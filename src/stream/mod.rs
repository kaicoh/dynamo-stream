mod client;
mod record;

#[cfg(test)]
mod mock;

use client::{DynamoStreamClient, StreamClient};
pub use record::{Record, Records};

use super::{Result, Subscriptions, ENV_DYNAMODB_ENDPOINT_URL};
use aws_sdk_dynamodbstreams::types::ShardIteratorType;
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};
use tracing::error;

pub async fn subscribe(state: Arc<Mutex<Subscriptions>>) {
    let endpoint_url = std::env::var(ENV_DYNAMODB_ENDPOINT_URL).ok();
    let client = DynamoStreamClient::builder()
        .await
        .endpoint_url(endpoint_url)
        .build();

    loop {
        sleep(Duration::from_secs(3)).await;

        let state = state.lock().unwrap();

        for (arn, subscriptions) in state.iter() {
            let client = client.clone();
            let arn = arn.clone();
            let subscriptions = subscriptions.clone();

            tokio::spawn(async move {
                let records = match get_records(&client, &arn).await {
                    Ok(_records) => _records,
                    Err(err) => {
                        error!("{err}");
                        return;
                    }
                };

                for sub in subscriptions {
                    let rs = records.clone();

                    tokio::spawn(async move {
                        if let Err(err) = sub.notify(&rs).await {
                            error!("{err}");
                        }
                    });
                }
            });
        }
    }
}

async fn get_records(client: &dyn StreamClient, stream_arn: &str) -> Result<Records> {
    let mut records: Vec<Record> = vec![];

    let (mut _records, mut last_evaluated_id) = get_record_iter(client, stream_arn, None).await?;
    records.append(&mut _records);

    while last_evaluated_id.is_some() {
        let (mut _records, _id) =
            get_record_iter(client, stream_arn, last_evaluated_id.take()).await?;
        records.append(&mut _records);

        last_evaluated_id = _id;
    }

    Ok(Records::new(records))
}

async fn get_record_iter(
    client: &dyn StreamClient,
    stream_arn: &str,
    last_evaluated_id: Option<String>,
) -> Result<(Vec<Record>, Option<String>)> {
    let mut records: Vec<Record> = vec![];
    let mut exclusive_id: Option<String> = None;

    if let Some(output) = client
        .describe_stream(stream_arn, last_evaluated_id)
        .await?
    {
        exclusive_id = output.last_evaluated_shard_id;

        for shard_id in output.shard_ids {
            let mut current_iter = client
                .get_shard_iterator(stream_arn, &shard_id, ShardIteratorType::Latest)
                .await?
                .shard_iterator;

            while current_iter.is_some() {
                let output = client.get_records(current_iter.take()).await?;

                let mut _records = output.records;
                records.append(&mut _records);

                current_iter = output.next_shard_iterator;
            }
        }
    }

    Ok((records, exclusive_id))
}

#[cfg(test)]
mod tests {
    use super::mock::{record_event_ids, shard_source_factory, ShardClient};
    use super::*;

    #[tokio::test]
    async fn test_get_record_iter() {
        let stream_arn = "test stream";
        let source = shard_source_factory();
        let client = ShardClient::new(stream_arn, source);

        let res = get_record_iter(&client, stream_arn, None).await;
        assert!(res.is_ok());

        let (records, exclusive_id) = res.unwrap();
        assert_eq!(exclusive_id, Some("shard_1".to_string()));
        assert_eq!(
            record_event_ids(records),
            ["a", "b", "c", "d", "e", "f", "g"],
        );

        let res = get_record_iter(&client, stream_arn, Some("shard_1".into())).await;
        assert!(res.is_ok());

        let (records, exclusive_id) = res.unwrap();
        assert!(exclusive_id.is_none());
        assert!(records.is_empty());
    }

    #[tokio::test]
    async fn test_get_records() {
        let stream_arn = "test stream";
        let source = shard_source_factory();
        let client = ShardClient::new(stream_arn, source);

        let res = get_records(&client, stream_arn).await;
        assert!(res.is_ok());

        let records = res.unwrap().into_inner();
        assert_eq!(
            record_event_ids(records),
            ["a", "b", "c", "d", "e", "f", "g"],
        );
    }
}
