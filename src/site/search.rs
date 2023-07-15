use crate::site::static_site::PackageWithIndex;
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use url::Url;

#[derive(Serialize)]
pub struct Entry<'a> {
    #[serde(rename = "objectID")]
    object_id: &'a String,
    title: &'a String,
    url: Url,
    last_updated: DateTime<Utc>,
    packages: usize,
}

pub fn create_search_index(
    to_path: &Path,
    items: &HashMap<String, Vec<PackageWithIndex>>,
) -> Result<(), anyhow::Error> {
    let index: Vec<_> = items
        .iter()
        .map(|(name, packages)| Entry {
            object_id: name,
            title: name,
            url: Url::parse(&format!("https://pypi.org/project/{}", name)).unwrap(),
            packages: packages.len(),
            last_updated: packages
                .iter()
                .map(|p| p.package.upload_time)
                .max()
                .unwrap(),
        })
        .collect();

    let mut writer = BufWriter::new(File::create(to_path)?);
    serde_json::to_writer(&mut writer, &index)?;

    Ok(())
}
