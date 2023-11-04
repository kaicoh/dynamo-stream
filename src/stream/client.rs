use super::super::Result;
use super::Record;

use async_trait::async_trait;
use aws_sdk_dynamodbstreams::{types::ShardIteratorType, Client};

#[derive(Debug)]
pub struct DescribeStreamResult {
    pub shard_ids: Vec<String>,
    pub last_evaluated_shard_id: Option<String>,
}

#[derive(Debug)]
pub struct GetShardIteratorResult {
    pub shard_iterator: Option<String>,
}

#[derive(Debug)]
pub struct GetRecordsResult {
    pub next_shard_iterator: Option<String>,
    pub records: Vec<Record>,
}

#[async_trait]
pub trait StreamClient: Send + Sync {
    async fn describe_stream(
        &self,
        stream_arn: &str,
        exclusive_id: Option<String>,
    ) -> Result<Option<DescribeStreamResult>>;

    async fn get_shard_iterator(
        &self,
        stream_arn: &str,
        shard_id: &str,
        shard_iterator_type: ShardIteratorType,
    ) -> Result<GetShardIteratorResult>;

    async fn get_records(&self, shard_iterator: Option<String>) -> Result<GetRecordsResult>;
}

#[derive(Debug, Clone)]
pub struct DynamoStreamClient {
    client: Client,
}

impl DynamoStreamClient {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl StreamClient for DynamoStreamClient {
    async fn describe_stream(
        &self,
        stream_arn: &str,
        exclusive_id: Option<String>,
    ) -> Result<Option<DescribeStreamResult>> {
        let opt = self
            .client
            .describe_stream()
            .stream_arn(stream_arn)
            .set_exclusive_start_shard_id(exclusive_id)
            .send()
            .await?
            .stream_description
            .map(|output| {
                let last_evaluated_shard_id = output.last_evaluated_shard_id;
                let shard_ids: Vec<String> = output
                    .shards
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|s| s.shard_id)
                    .collect();

                DescribeStreamResult {
                    last_evaluated_shard_id,
                    shard_ids,
                }
            });

        Ok(opt)
    }

    async fn get_shard_iterator(
        &self,
        stream_arn: &str,
        shard_id: &str,
        shard_iterator_type: ShardIteratorType,
    ) -> Result<GetShardIteratorResult> {
        let shard_iterator = self
            .client
            .get_shard_iterator()
            .stream_arn(stream_arn)
            .shard_id(shard_id)
            .shard_iterator_type(shard_iterator_type)
            .send()
            .await?
            .shard_iterator;

        Ok(GetShardIteratorResult { shard_iterator })
    }

    async fn get_records(&self, shard_iterator: Option<String>) -> Result<GetRecordsResult> {
        let output = self
            .client
            .get_records()
            .set_shard_iterator(shard_iterator)
            .send()
            .await?;

        let records = output
            .records
            .unwrap_or_default()
            .into_iter()
            .map(Record::from)
            .collect();

        Ok(GetRecordsResult {
            records,
            next_shard_iterator: output.next_shard_iterator,
        })
    }
}
