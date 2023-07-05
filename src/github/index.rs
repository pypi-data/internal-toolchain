use crate::github::{get_client, GithubError};
use crate::repository::index::RepositoryIndex;
use anyhow::Context;
use base64::engine::general_purpose;
use base64::Engine;
use serde::Serialize;
use std::fs::File;
use std::io;
use std::io::BufReader;
use std::path::Path;
use ureq::Agent;

pub fn get_repository_index(
    token: &str,
    name: &str,
    client: Option<Agent>,
) -> Result<RepositoryIndex, GithubError> {
    let client = client.unwrap_or_else(get_client);

    let response = client
        .get(&format!(
            "https://api.github.com/repos/pypi-data/{name}/contents/index.json"
        ))
        .set("Authorization", &format!("bearer {token}"))
        .set("X-GitHub-Api-Version", "2022-11-28")
        .set("Accept", "application/vnd.github.raw")
        .call()
        .map_err(Box::new)?;

    Ok(serde_json::from_str(&response.into_string()?)
        .with_context(|| format!("Error getting index content for {name}"))?)
}

#[derive(Serialize)]
pub struct PutFile {
    message: String,
    content: String,
}

pub fn upload_index_file(
    client: &Agent,
    token: &str,
    name_with_owner: &str,
    path: &Path,
) -> Result<(), GithubError> {
    let reader = BufReader::new(File::open(path)?);
    let contents = io::read_to_string(reader)?;

    let put_file = PutFile {
        message: "Adding index".to_string(),
        content: general_purpose::STANDARD.encode(contents),
    };

    client
        .put(&format!(
            "https://api.github.com/repos/{name_with_owner}/contents/index.json"
        ))
        .set("Authorization", &format!("bearer {token}"))
        .set("X-GitHub-Api-Version", "2022-11-28")
        .set("Accept", "application/vnd.github+json")
        .set("Content-Type", "application/json")
        .send_json(put_file)
        .map_err(Box::new)?;
    Ok(())
}
