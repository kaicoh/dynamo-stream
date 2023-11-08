use super::{Client, GetRecordsOutput, GetShardsOutput, Record, Shard};

use anyhow::Result;
use aws_sdk_dynamodb::{config::Builder as DbConfigBuilder, Client as DbClient};
use aws_sdk_dynamodbstreams::{
    config::Builder as StreamConfigBuilder, types::ShardIteratorType, Client as StreamClient,
};
use axum::async_trait;

#[derive(Debug, Clone)]
pub struct DynamodbClient {
    db_client: DbClient,
    stream_client: StreamClient,
}

#[async_trait]
impl Client for DynamodbClient {
    async fn get_shards(&self, table_name: &str) -> Result<GetShardsOutput> {
        let mut shards: Vec<Shard> = vec![];
        let stream_arn = self.get_stream_arn(table_name).await?;
        let (shard_ids, mut next_id) = self.describe_stream(&stream_arn, None).await?;

        let mut _shards = self.map_to_shards(&stream_arn, &shard_ids).await?;
        shards.append(&mut _shards);

        while next_id.is_some() {
            let (shard_ids, _next_id) = self.describe_stream(&stream_arn, next_id.clone()).await?;

            let mut _shards = self.map_to_shards(&stream_arn, &shard_ids).await?;
            shards.append(&mut _shards);

            next_id = _next_id
        }

        Ok(GetShardsOutput { shards })
    }

    async fn get_records(&self, iterator: &str) -> Result<GetRecordsOutput> {
        self.stream_client
            .get_records()
            .shard_iterator(iterator)
            .send()
            .await
            .map(|output| {
                let next_iterator = output.next_shard_iterator;
                let records = output
                    .records
                    .unwrap_or_default()
                    .into_iter()
                    .map(Record::from)
                    .collect();

                GetRecordsOutput {
                    records,
                    next_iterator,
                }
            })
            .map_err(anyhow::Error::from)
    }
}

impl DynamodbClient {
    pub async fn builder() -> DynamodbClientBuilder {
        DynamodbClientBuilder::new().await
    }

    async fn get_stream_arn(&self, table_name: &str) -> Result<String> {
        self.db_client
            .describe_table()
            .table_name(table_name)
            .send()
            .await?
            .table
            .ok_or(anyhow::anyhow!("`table` is None in `DescribeTableOutput`"))?
            .latest_stream_arn
            .ok_or(anyhow::anyhow!(
                "`latest_stream_arn` is None in `TableDescription`"
            ))
    }

    async fn describe_stream(
        &self,
        stream_arn: &str,
        last_evaluated_shard_id: Option<String>,
    ) -> Result<(Vec<String>, Option<String>)> {
        self.stream_client
            .describe_stream()
            .stream_arn(stream_arn)
            .set_exclusive_start_shard_id(last_evaluated_shard_id)
            .send()
            .await?
            .stream_description
            .ok_or(anyhow::anyhow!(
                "`stream_description` is None in `DescribeStreamOutput`"
            ))
            .map(|output| {
                let last_evaluated_shard_id = output.last_evaluated_shard_id;
                let shard_ids: Vec<String> = output
                    .shards
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|shard| shard.shard_id)
                    .collect();
                (shard_ids, last_evaluated_shard_id)
            })
    }

    async fn get_shard_iterator(&self, stream_arn: &str, shard_id: &str) -> Result<Option<String>> {
        self.stream_client
            .get_shard_iterator()
            .stream_arn(stream_arn)
            .shard_id(shard_id)
            .shard_iterator_type(ShardIteratorType::Latest)
            .send()
            .await
            .map(|output| output.shard_iterator)
            .map_err(anyhow::Error::from)
    }

    async fn map_to_shards(&self, stream_arn: &str, shard_ids: &[String]) -> Result<Vec<Shard>> {
        let mut shards: Vec<Shard> = vec![];

        for shard_id in shard_ids {
            if let Some(iterator) = self
                .get_shard_iterator(stream_arn, shard_id.as_str())
                .await?
            {
                let shard = Shard::new(shard_id, iterator);
                shards.push(shard);
            }
        }

        Ok(shards)
    }
}

#[derive(Debug)]
pub struct DynamodbClientBuilder {
    db_builder: DbConfigBuilder,
    stream_builder: StreamConfigBuilder,
}

impl DynamodbClientBuilder {
    pub async fn new() -> Self {
        let config = aws_config::load_from_env().await;
        let db_builder = DbConfigBuilder::from(&config);
        let stream_builder = StreamConfigBuilder::from(&config);

        Self {
            db_builder,
            stream_builder,
        }
    }

    pub fn endpoint_url(self, url: Option<String>) -> Self {
        match url {
            Some(url) => {
                let db_builder = self.db_builder.endpoint_url(&url);
                let stream_builder = self.stream_builder.endpoint_url(&url);
                Self {
                    db_builder,
                    stream_builder,
                }
            }
            None => self,
        }
    }

    pub fn build(self) -> DynamodbClient {
        let db_config = self.db_builder.build();
        let db_client = DbClient::from_conf(db_config);

        let stream_config = self.stream_builder.build();
        let stream_client = StreamClient::from_conf(stream_config);

        DynamodbClient {
            db_client,
            stream_client,
        }
    }
}
