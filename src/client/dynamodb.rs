use super::{Client, GetRecordsOutput, GetShardsOutput, Record, Records, Shards};

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
        let mut shards = Shards::new();
        let stream_arn = self.get_stream_arn(table_name).await?;
        let mut next_id = self.append_shards(&mut shards, &stream_arn, None).await?;

        while next_id.is_some() {
            next_id = self
                .append_shards(&mut shards, &stream_arn, next_id.clone())
                .await?;
        }

        Ok(GetShardsOutput { shards })
    }

    async fn get_records(&self, shards: &Shards) -> Result<GetRecordsOutput> {
        let mut next_shards = Shards::new();
        let mut records = Records::new(vec![]);

        for (shard_id, iterator_id) in shards.iter() {
            if let Some(next_iterator_id) = self.append_records(&mut records, iterator_id).await? {
                next_shards.insert(shard_id, next_iterator_id);
            }
        }

        Ok(GetRecordsOutput {
            shards: next_shards,
            records,
        })
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

    async fn append_shards(
        &self,
        shards: &mut Shards,
        stream_arn: &str,
        last_evaluated_shard_id: Option<String>,
    ) -> Result<Option<String>> {
        let (shard_ids, next_id) = self
            .stream_client
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
                let next_id = output.last_evaluated_shard_id;
                let shard_ids: Vec<String> = output
                    .shards
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|shard| shard.shard_id)
                    .collect();
                (shard_ids, next_id)
            })?;

        for shard_id in shard_ids {
            if let Some(iterator_id) = self
                .stream_client
                .get_shard_iterator()
                .stream_arn(stream_arn)
                .shard_id(&shard_id)
                .shard_iterator_type(ShardIteratorType::Latest)
                .send()
                .await
                .map(|output| output.shard_iterator)?
            {
                shards.insert(shard_id, iterator_id);
            }
        }

        Ok(next_id)
    }

    async fn append_records(
        &self,
        records: &mut Records,
        iterator: &str,
    ) -> Result<Option<String>> {
        let (mut _records, next_iterator_id) = self
            .stream_client
            .get_records()
            .shard_iterator(iterator)
            .send()
            .await
            .map(|output| {
                let next_iterator_id = output.next_shard_iterator;
                let _records = output
                    .records
                    .unwrap_or_default()
                    .into_iter()
                    .map(Record::from)
                    .collect();
                (_records, next_iterator_id)
            })?;

        records.append(&mut _records);

        Ok(next_iterator_id)
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
