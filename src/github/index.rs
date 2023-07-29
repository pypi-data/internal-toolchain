use crate::github::{get_client, GithubError};
use crate::repository::index::RepositoryIndex;
use anyhow::Context;
use base64::engine::general_purpose;
use base64::Engine;
use serde::{Deserialize, Serialize};

use ureq::{Agent, Error};

pub fn get_repository_index(
    name: &str,
    client: Option<Agent>,
) -> Result<RepositoryIndex, GithubError> {
    let client = client.unwrap_or_else(get_client);

    let response = client
        .get(&format!(
            "https://raw.githubusercontent.com/pypi-data/{name}/main/index.json",
        ))
        .call()
        .map_err(Box::new)?;

    Ok(serde_json::from_reader(response.into_reader())
        .with_context(|| format!("Error getting index content for {name}"))?)
}

#[derive(Serialize)]
pub struct PutFile {
    message: String,
    content: String,
    sha: Option<String>,
}

#[derive(Deserialize)]
pub struct GetFile {
    sha: String,
}

// enum UploadFile {
//     Path(Path),
//     Contents(String)
// }

pub fn upload_index_file(
    client: &Agent,
    token: &str,
    name_with_owner: &str,
    // file: UploadFile,
    contents: String,
) -> Result<(), GithubError> {
    // let reader = BufReader::new(File::open(path)?);
    // let contents = io::read_to_string(reader)?;
    let blob_sha = match client
        .get(&format!(
            "https://api.github.com/repos/{name_with_owner}/contents/index.json"
        ))
        .call()
    {
        Ok(r) => {
            let get_resp: GetFile = r.into_json()?;
            Some(get_resp.sha)
        }
        Err(_) => None,
    };

    let put_file = PutFile {
        message: "Adding index".to_string(),
        content: general_purpose::STANDARD.encode(contents),
        sha: blob_sha,
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
        .map_err(Box::new)
        .map_err(|e| {
            match *e {
                Error::Status(status, r) => {
                    let contents = r.into_string().unwrap();
                    panic!("Error: Status {status}. Response: {contents}");
                }
                _ => {
                    return e
                }
            }
        })?;
    Ok(())
}
