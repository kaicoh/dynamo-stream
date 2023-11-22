use super::{client::Client, shard::Shard, types::Records};

use async_recursion::async_recursion;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tracing::error;

#[derive(Debug, Clone)]
pub struct Lineage {
    shard: Shard,

    #[cfg(test)]
    pub children: Vec<Lineage>,

    #[cfg(not(test))]
    children: Vec<Lineage>,
}

impl Lineage {
    pub fn new(shard: Shard) -> Self {
        Self {
            shard,
            children: vec![],
        }
    }

    pub fn shard_id(&self) -> &str {
        self.shard.id()
    }

    pub fn parent(&self) -> Option<&str> {
        self.shard.parent()
    }

    pub fn set_children(&mut self, children: Vec<Lineage>) {
        self.children = children;
    }

    pub fn has(&self, shard_id: Option<&str>) -> bool {
        if let Some(id) = shard_id {
            if self.shard_id() == id {
                true
            } else {
                self.children.iter().any(|child| child.has(shard_id))
            }
        } else {
            false
        }
    }

    pub fn is_child(&self, shard_id: &str) -> bool {
        if let Some(parent_id) = self.parent() {
            parent_id == shard_id
        } else {
            false
        }
    }

    pub fn set_descendant(&mut self, desc: &Lineage) {
        if let Some(parent_id) = desc.parent() {
            if self.shard_id() == parent_id {
                self.children.push(desc.clone());
            } else {
                for lineage in self.children.iter_mut() {
                    lineage.set_descendant(desc);
                }
            }
        }
    }

    #[async_recursion]
    pub async fn get_records(self, client: Arc<dyn Client>, tx: Sender<(Option<Shard>, Records)>) {
        let Lineage { shard, children } = self;

        let result = shard
            .get_records(Arc::clone(&client))
            .await
            .unwrap_or_else(|err| {
                error!("Failed to get records from shard: {:#?}", err);
                (None, Records::default())
            });

        if let Err(err) = tx.send(result).await {
            error!("Failed to send get records result: {:#?}", err);
        }

        for child in children {
            let client = Arc::clone(&client);
            let tx = tx.clone();

            tokio::spawn(async move {
                child.get_records(client, tx).await;
            });
        }
    }
}
