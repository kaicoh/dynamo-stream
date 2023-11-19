use super::{
    client::{Client, GetRecordsOutput},
    types::Records,
};

use anyhow::Result;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Shard {
    id: String,
    iterator: Option<String>,
    parent: Option<String>,
}

impl Shard {
    pub fn new<T: Into<String>>(id: &str, parent: Option<T>) -> Self {
        Self {
            id: id.into(),
            iterator: None,
            parent: parent.map(|s| s.into()),
        }
    }

    pub fn id(&self) -> &str {
        self.id.as_str()
    }

    pub fn parent(&self) -> Option<&str> {
        self.parent.as_deref()
    }

    pub async fn set_iterator(
        &mut self,
        client: Arc<dyn Client>,
        stream_arn: &str,
    ) -> Result<()> {
        let output = client.get_iterator(stream_arn, self.id()).await?;
        self.iterator = output.iterator;
        Ok(())
    }

    pub async fn get_records(self, client: Arc<dyn Client>) -> Result<(Option<Shard>, Records)> {
        match self.iterator.as_deref() {
            Some(iterator) => {
                let GetRecordsOutput {
                    records,
                    next_iterator
                } = client.get_records(iterator).await?;

                let shard = next_iterator
                    .map(|iterator| Shard {
                        iterator: Some(iterator),
                        ..self
                    });

                Ok((shard, records))
            },
            None => Ok((None, Records::new())),
        }
    }
}
