use serde::Serialize;
use std::collections::{HashMap, hash_map::Iter};

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Subscription {
    /// The url post to when accepting the DynamoDB Stream
    url: String,
}

impl Subscription {
    pub fn new<T: Into<String>>(url: T) -> Self {
        Self {
            url: url.into()
        }
    }

    pub fn url(&self) -> &str {
        self.url.as_str()
    }
}

/// A HashMap whose key is DynamoDB Stream Arn and whose value is a list of configurations
/// to inform to the destinations.
type Subscriptions = HashMap<String, Vec<Subscription>>;

#[derive(Debug, Clone, Serialize)]
#[serde(transparent)]
pub struct AppState {
    configs: Subscriptions,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            configs: HashMap::new(),
        }
    }

    /// Register Http POST destination to a DynamoDB Stream Arn.
    pub fn insert<K: Into<String>>(&mut self, arn: K, config: Subscription) {
        let arn: String = arn.into();

        if !self.include(&arn, &config) {
            let mut values = self.configs
                .get(&arn)
                .unwrap_or(&Vec::new())
                .to_vec();
            values.push(config);
            self.configs.insert(arn, values);
        }
    }

    /// Iterates using pairs from DynamoDB Stream Arn and its destinations.
    pub fn iter(&self) -> Iter<'_, String, Vec<Subscription>> {
        self.configs.iter()
    }

    fn include(&self, arn: &str, config: &Subscription) -> bool {
        self.configs
            .get(arn)
            .map(|values| {
                values.iter().any(|v| v == config)
            })
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_adds_string_using_isert_method() {
        let mut configs: Subscriptions = HashMap::new();
        configs.insert("key".into(), from_slice(&["val_0"]));

        let mut state = AppState { configs };
        state.insert("key", Subscription::new("val_1"));

        let values = state.configs.get("key");
        assert!(values.is_some());
        assert_eq!(
            values.unwrap().to_vec(),
            vec![Subscription::new("val_0"), Subscription::new("val_1")],
        );
    }

    #[test]
    fn it_does_not_add_duplicate_string_using_insert_method() {
        let mut configs: Subscriptions = HashMap::new();
        configs.insert("key".into(), from_slice(&["val_0"]));

        let mut state = AppState { configs };
        state.insert("key", Subscription::new("val_0"));

        let values = state.configs.get("key");
        assert!(values.is_some());
        assert_eq!(
            values.unwrap().to_vec(),
            vec![Subscription::new("val_0")]
        );
    }

    #[test]
    fn it_iterates_over_subscriptions() {
        let mut configs: Subscriptions = HashMap::new();
        configs.insert("key_0".into(), from_slice(&["val_0", "val_1"]));
        configs.insert("key_1".into(), from_slice(&["val_2", "val_3"]));

        let state = AppState { configs };

        let iterator = state.iter();
        assert_eq!(iterator.len(), 2);

        for item in iterator {
            match item.0.as_str() {
                "key_0" => {
                    assert_eq!(
                        item.1,
                        &from_slice(&["val_0", "val_1"]),
                    );
                },
                "key_1" => {
                    assert_eq!(
                        item.1,
                        &from_slice(&["val_2", "val_3"]),
                    );
                },
                _ => {
                    unreachable!();
                }
            }
        }
    }

    fn from_slice(urls: &[&str]) -> Vec<Subscription> {
        urls.iter().map(|&v| Subscription::new(v)).collect()
    }
}
