use crate::{ENV_CONFIG_PATH, ENV_DYNAMODB_ENDPOINT_URL, ENV_PORT};

use std::env;

mod file;

use file::ConfigFile;
pub use file::EntryConfig;

#[derive(Debug)]
pub struct Config {
    endpoint_url: Option<String>,
    port: u16,
    entries: Vec<EntryConfig>,
}

impl Config {
    pub fn new() -> Self {
        let endpoint_url = env::var(ENV_DYNAMODB_ENDPOINT_URL).ok();
        let port = env::var(ENV_PORT)
            .ok()
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(3000);

        let conf_path = env::var(ENV_CONFIG_PATH).ok();
        let file = ConfigFile::new(conf_path);

        Self {
            endpoint_url,
            port,
            entries: file.entries(),
        }
    }

    pub fn endpoint_url(&self) -> Option<String> {
        self.endpoint_url.clone()
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn entries(&self) -> Vec<EntryConfig> {
        self.entries.clone()
    }
}

impl Default for Config {
    fn default() -> Config {
        Config::new()
    }
}
