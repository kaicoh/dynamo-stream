use super::{client::Client, lineage::Lineage, shard::Shard, types::Records};

use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct Lineages {
    shard_len: usize,
    lineages: Vec<Lineage>,
}

impl Lineages {
    pub fn len(&self) -> usize {
        self.lineages.len()
    }

    pub fn has(&self, shard_id: &str) -> bool {
        self.lineages
            .iter()
            .any(|lineage| lineage.has(Some(shard_id)))
    }

    pub fn set_shard(&mut self, shard: Shard) {
        push_shard_to_lineages(&mut self.lineages, shard);
    }

    pub async fn get_records(self, client: Arc<dyn Client>) -> (Records, Vec<Shard>) {
        let mut records = Records::new();
        let mut shards: Vec<Shard> = vec![];

        let (tx, mut rx) = mpsc::channel::<(Option<Shard>, Records)>(self.shard_len);

        for lineage in self.lineages {
            let client = Arc::clone(&client);
            let tx = tx.clone();

            tokio::spawn(async move {
                lineage.get_records(client, tx).await;
            });
        }

        drop(tx);

        while let Some((_shard, mut _records)) = rx.recv().await {
            if let Some(_shard) = _shard {
                shards.push(_shard);
            }

            if !_records.is_empty() {
                records.append(&mut _records);
            }
        }

        (records, shards)
    }
}

impl From<Vec<Shard>> for Lineages {
    fn from(shards: Vec<Shard>) -> Self {
        Self {
            shard_len: shards.len(),
            lineages: shards_to_lineages(shards),
        }
    }
}

fn shards_to_lineages(shards: Vec<Shard>) -> Vec<Lineage> {
    let mut lineages: Vec<Lineage> = vec![];

    for shard in shards {
        push_shard_to_lineages(&mut lineages, shard);
    }

    lineages
}

fn push_shard_to_lineages(lineages: &mut Vec<Lineage>, shard: Shard) {
    // Group lineages into children of the shard and its ramainings.
    let (children, remains): (Vec<Lineage>, Vec<Lineage>) = lineages
        .clone()
        .into_iter()
        .partition(|lineage| lineage.is_child(shard.id()));

    *lineages = remains;

    let mut lineage = Lineage::new(shard.clone());
    lineage.set_children(children);

    // Search the lineage which the shard should belong to
    if let Some(ancestor) = lineages
        .iter_mut()
        .find(|lineage| lineage.has(shard.parent()))
    {
        // Set as a descendant of an ancestor lineage
        ancestor.set_descendant(&lineage);
    } else {
        // Add as new and independant lineage
        lineages.push(lineage);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::Itertools;

    //     0
    //  / \  \
    //  1  2  3
    // / \  \
    // 4  5  6
    //     /  \
    //    7    8
    #[test]
    fn it_transforms_into_single_lineage_from_shards() {
        let s0 = Shard::new::<&str>("0", None);
        let s1 = Shard::new("1", Some("0"));
        let s2 = Shard::new("2", Some("0"));
        let s3 = Shard::new("3", Some("0"));
        let s4 = Shard::new("4", Some("1"));
        let s5 = Shard::new("5", Some("1"));
        let s6 = Shard::new("6", Some("2"));
        let s7 = Shard::new("7", Some("6"));
        let s8 = Shard::new("8", Some("6"));

        for shards in [s0, s1, s2, s3, s4, s5, s6, s7, s8]
            .into_iter()
            .permutations(9)
        {
            assert_eq!(shards.len(), 9);

            let mut lineages = shards_to_lineages(shards);
            assert_eq!(lineages.len(), 1);

            let l0 = lineages.pop().unwrap();
            assert_eq!(l0.shard_id(), "0");
            assert_eq!(l0.children.len(), 3);

            let l1 = get_child(&l0, "1");
            let l2 = get_child(&l0, "2");
            let l3 = get_child(&l0, "3");
            assert!(l1.is_some());
            assert!(l2.is_some());
            assert!(l3.is_some());

            let l1 = l1.unwrap();
            let l2 = l2.unwrap();
            let l3 = l3.unwrap();
            assert_eq!(l1.children.len(), 2);
            assert_eq!(l2.children.len(), 1);
            assert_eq!(l3.children.len(), 0);

            let l4 = get_child(&l1, "4");
            let l5 = get_child(&l1, "5");
            let l6 = get_child(&l2, "6");
            assert!(l4.is_some());
            assert!(l5.is_some());
            assert!(l6.is_some());

            let l4 = l4.unwrap();
            let l5 = l5.unwrap();
            let l6 = l6.unwrap();
            assert_eq!(l4.children.len(), 0);
            assert_eq!(l5.children.len(), 0);
            assert_eq!(l6.children.len(), 2);

            let l7 = get_child(&l6, "7");
            let l8 = get_child(&l6, "8");
            assert!(l7.is_some());
            assert!(l8.is_some());

            let l7 = l7.unwrap();
            let l8 = l8.unwrap();
            assert_eq!(l7.children.len(), 0);
            assert_eq!(l8.children.len(), 0);
        }
    }

    //     0       3
    //   /  \    /  \
    //  1   2   4    5
    #[test]
    fn it_transforms_into_multiple_lineages_from_shards() {
        let s0 = Shard::new::<&str>("0", None);
        let s1 = Shard::new("1", Some("0"));
        let s2 = Shard::new("2", Some("0"));
        let s3 = Shard::new::<&str>("3", None);
        let s4 = Shard::new("4", Some("3"));
        let s5 = Shard::new("5", Some("3"));

        for shards in [s0, s1, s2, s3, s4, s5].into_iter().permutations(6) {
            assert_eq!(shards.len(), 6);

            let lineages = shards_to_lineages(shards);
            assert_eq!(lineages.len(), 2);

            let l0 = lineages.iter().find(|l| l.shard_id() == "0").unwrap();
            let l3 = lineages.iter().find(|l| l.shard_id() == "3").unwrap();

            let l1 = get_child(&l0, "1");
            let l2 = get_child(&l0, "2");
            let l4 = get_child(&l3, "4");
            let l5 = get_child(&l3, "5");
            assert!(l1.is_some());
            assert!(l2.is_some());
            assert!(l4.is_some());
            assert!(l5.is_some());
        }
    }

    fn get_child(lineage: &Lineage, shard_id: &str) -> Option<Lineage> {
        lineage
            .children
            .iter()
            .find(|l| l.shard_id() == shard_id)
            .cloned()
    }
}
