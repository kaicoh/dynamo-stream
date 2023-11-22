use serde::Deserialize;
use std::fs;
use std::path::Path;
use tracing::warn;

#[derive(Debug, Default, Deserialize)]
pub struct ConfigFile {
    entries: Option<Vec<Entry>>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct Entry {
    pub table_name: String,
    pub url: String,
}

impl ConfigFile {
    pub fn new<P: AsRef<Path>>(path: Option<P>) -> Self {
        path.map(read_config).unwrap_or_default()
    }

    pub fn entries(&self) -> Vec<Entry> {
        self.entries.clone().unwrap_or_default()
    }
}

fn read_config<P: AsRef<Path>>(path: P) -> ConfigFile {
    _read_config(path).unwrap_or_else(|err| {
        warn!("{err}");
        warn!("Skip reading config file.");
        ConfigFile::default()
    })
}

fn _read_config<P: AsRef<Path>>(path: P) -> Result<ConfigFile, String> {
    let content = fs::read_to_string(&path)
        .map_err(|err| format!("Failed to read: {}. {err}", path.as_ref().to_string_lossy()))?;
    serde_yaml::from_str(&content)
        .map_err(|err| format!("Failed to deserialize config file: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_loads_config() {
        let result = _read_config("src/web/config/test/valid.yml");
        assert!(result.is_ok());

        let config = result.unwrap();
        assert!(config.entries.is_some());
        assert_eq!(config.entries().len(), 2);

        assert_eq!(
            config.entries().get(0).unwrap(),
            &Entry {
                table_name: "People".into(),
                url: "http://localhost:8888".into(),
            }
        );

        assert_eq!(
            config.entries().get(1).unwrap(),
            &Entry {
                table_name: "User".into(),
                url: "http://localhost:4000".into(),
            }
        );
    }

    #[test]
    fn it_returns_err_if_the_file_does_not_exist() {
        let result = _read_config("src/web/config/test/non-exist.yml");
        assert!(result.is_err());

        let message = result.unwrap_err();
        assert_eq!(
            message,
            "Failed to read: src/web/config/test/non-exist.yml. No such file or directory (os error 2)"
        );
    }

    #[test]
    fn it_returns_err_if_the_file_is_invalid() {
        let result = _read_config("src/web/config/test/invalid.yml");
        assert!(result.is_err());

        let message = result.unwrap_err();
        assert_eq!(
            message,
            "Failed to deserialize config file: entries[0]: missing field `url` at line 2 column 5"
        );
    }
}
