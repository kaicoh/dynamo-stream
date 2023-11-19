use super::{
    Client, GetIteratorOutput, GetRecordsOutput, GetShardsOutput, GetStreamArnOutput, Record,
    Records, Shard,
};

use anyhow::Result;
use aws_sdk_dynamodb::{config::Builder as DbConfigBuilder, Client as DbClient};
use aws_sdk_dynamodbstreams::{
    config::Builder as StreamConfigBuilder,
    error::SdkError,
    operation::{get_records::GetRecordsError, get_shard_iterator::GetShardIteratorError},
    types::{ShardIteratorType, StreamDescription},
    Client as StreamClient,
};
use axum::async_trait;
use tracing::warn;

#[derive(Debug, Clone)]
pub struct DynamodbClient {
    db_client: DbClient,
    stream_client: StreamClient,
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

#[async_trait]
impl Client for DynamodbClient {
    async fn get_iterator(&self, stream_arn: &str, shard_id: &str) -> Result<GetIteratorOutput> {
        self.stream_client
            .get_shard_iterator()
            .stream_arn(stream_arn)
            .shard_id(shard_id)
            .shard_iterator_type(ShardIteratorType::Latest)
            .send()
            .await
            .map(|output| GetIteratorOutput {
                iterator: output.shard_iterator,
            })
            .or_else(from_get_iterator_err)
    }

    async fn get_records(&self, iterator: &str) -> Result<GetRecordsOutput> {
        self.stream_client
            .get_records()
            .shard_iterator(iterator)
            .send()
            .await
            .map(|output| {
                let records = output
                    .records
                    .unwrap_or_default()
                    .into_iter()
                    .map(Record::from)
                    .collect::<Vec<Record>>()
                    .into();
                let next_iterator = output.next_shard_iterator;

                GetRecordsOutput {
                    records,
                    next_iterator,
                }
            })
            .or_else(from_get_records_err)
    }

    async fn get_shards(
        &self,
        stream_arn: &str,
        exclusive_shard_id: Option<String>,
    ) -> Result<GetShardsOutput> {
        self.stream_client
            .describe_stream()
            .stream_arn(stream_arn)
            .set_exclusive_start_shard_id(exclusive_shard_id)
            .send()
            .await
            .map_err(anyhow::Error::from)
            .and_then(|output| {
                output
                    .stream_description
                    .ok_or(anyhow::anyhow!("Stream Description is None"))
            })
            .map(|output| {
                let StreamDescription {
                    stream_status,
                    stream_view_type,
                    table_name,
                    shards,
                    last_evaluated_shard_id,
                    ..
                } = output;

                let shards = shards
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|s| {
                        let shard_id = s.shard_id;
                        let parent = s.parent_shard_id;

                        shard_id.map(|id| Shard::new(id.as_str(), parent))
                    })
                    .collect::<Vec<Shard>>();

                GetShardsOutput {
                    shards,
                    last_shard_id: last_evaluated_shard_id,
                }
            })
    }

    async fn get_stream_arn(&self, table: &str) -> Result<GetStreamArnOutput> {
        self.db_client
            .describe_table()
            .table_name(table)
            .send()
            .await?
            .table
            .ok_or(anyhow::anyhow!("`table` is None in `DescribeTableOutput`"))?
            .latest_stream_arn
            .map(|stream_arn| GetStreamArnOutput { stream_arn })
            .ok_or(anyhow::anyhow!(
                "`latest_stream_arn` is None in `TableDescription`"
            ))
    }
}

fn from_get_iterator_err(err: SdkError<GetShardIteratorError>) -> Result<GetIteratorOutput> {
    use GetShardIteratorError::*;

    match err {
        SdkError::ServiceError(e) => {
            let e = e.into_err();
            match e {
                // Close shard if response is either ResourceNotFound or TrimmedDataAccess
                ResourceNotFoundException(_) | TrimmedDataAccessException(_) => {
                    warn!("GetShardIterator operation failed due to {e}");
                    warn!("{:#?}", e);
                    Ok(GetIteratorOutput { iterator: None })
                }
                _ => Err(anyhow::Error::from(e)),
            }
        }
        _ => Err(anyhow::Error::from(err)),
    }
}

fn from_get_records_err(err: SdkError<GetRecordsError>) -> Result<GetRecordsOutput> {
    use GetRecordsError::*;

    match err {
        SdkError::ServiceError(e) => {
            let e = e.into_err();
            match e {
                // Close shard if response is one of ExpiredIterator, LimitExceeded
                // ResourceNotFound and TrimmedDataAccess.
                ExpiredIteratorException(_)
                | LimitExceededException(_)
                | ResourceNotFoundException(_)
                | TrimmedDataAccessException(_) => {
                    warn!("GetRecords operation failed due to {e}");
                    warn!("{:#?}", e);
                    Ok(GetRecordsOutput {
                        records: Records::new(),
                        next_iterator: None,
                    })
                }
                _ => Err(anyhow::Error::from(e)),
            }
        }
        _ => Err(anyhow::Error::from(err)),
    }
}
