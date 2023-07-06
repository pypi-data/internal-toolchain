use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use url::Url;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct RepositoryPackage {
    pub project_name: String,
    pub project_version: String,
    pub url: Url,
    pub upload_time: DateTime<Utc>,
    pub processed: bool,
}

impl RepositoryPackage {
    pub fn package_filename(&self) -> &str {
        self.url.path_segments().unwrap().last().unwrap()
    }

    pub fn set_processed(&mut self, value: bool) {
        self.processed = value;
    }

    pub fn identifier(&self) -> String {
        format!(
            "{}/{}/{}",
            self.project_name,
            self.project_version,
            self.package_filename()
        )
    }

    pub fn file_prefix(&self) -> String {
        format!(
            "packages/{}/{}/",
            self.project_name,
            self.package_filename()
        )
    }

    pub fn fake_from_url(url: Url) -> Self {
        RepositoryPackage {
            project_name: "fake".to_string(),
            project_version: "fake".to_string(),
            url,
            upload_time: Default::default(),
            processed: false,
        }
    }
}

impl Display for RepositoryPackage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.identifier())
    }
}
