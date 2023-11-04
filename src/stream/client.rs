use super::super::Result;
use super::Record;

use async_trait::async_trait;
use aws_sdk_dynamodbstreams::{config::Builder as ConfigBuilder, types::ShardIteratorType, Client};

pub struct DescribeStreamResult {
    pub shard_ids: Box<dyn Iterator<Item = String> + Send + Sync>,
    pub last_evaluated_shard_id: Option<String>,
}

#[derive(Debug, Default, Clone)]
pub struct GetShardIteratorResult {
    pub shard_iterator: Option<String>,
}

#[derive(Debug, Default, Clone)]
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

    pub async fn builder() -> DynamoStreamClientBuilder {
        DynamoStreamClientBuilder::new().await
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
                let shard_ids = output
                    .shards
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|s| s.shard_id);

                DescribeStreamResult {
                    last_evaluated_shard_id,
                    shard_ids: Box::new(shard_ids),
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

#[derive(Debug)]
pub struct DynamoStreamClientBuilder {
    builder: ConfigBuilder,
}

impl DynamoStreamClientBuilder {
    pub async fn new() -> Self {
        let config = aws_config::load_from_env().await;
        let builder = ConfigBuilder::from(&config);
        Self { builder }
    }

    pub fn endpoint_url<T: Into<String>>(self, url: Option<T>) -> Self {
        if let Some(url) = url {
            let builder = self.builder.endpoint_url(url.into());
            Self { builder }
        } else {
            self
        }
    }

    pub fn build(self) -> DynamoStreamClient {
        let config = self.builder.build();
        let client = Client::from_conf(config);
        DynamoStreamClient::new(client)
    }
}
