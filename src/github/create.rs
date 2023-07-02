use std::fs::File;
use std::io;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use crate::github::{get_client, GithubError};
use graphql_client::{GraphQLQuery, Response};
use serde::Serialize;
use ureq::Error;
use base64::{Engine as _, engine::general_purpose};

#[derive(GraphQLQuery)]
#[graphql(
schema_path = "src/github/schema.graphql",
query_path = "src/github/get_template_data.graphql",
response_derives = "Debug"
)]
pub struct GetTemplateData;

#[derive(Debug)]
pub struct TemplateData {
    repo_id: String,
    owner_id: String,
}

pub fn get_template_data(token: &str) -> Result<TemplateData, GithubError> {
    let client = get_client();
    let variables = get_template_data::Variables {};
    let request_body = GetTemplateData::build_query(variables);
    let response = client
        .post("https://api.github.com/graphql")
        .set("Authorization", &format!("bearer {token}"))
        .send_json(request_body)?;
    let body: Response<get_template_data::ResponseData> = response.into_json()?;
    let repo = body
        .data
        .and_then(|d| d.repository)
        .ok_or(GithubError::InvalidResponse)?;
    Ok(TemplateData {
        repo_id: repo.id,
        owner_id: repo.owner.id,
    })
}

#[derive(GraphQLQuery)]
#[graphql(
schema_path = "src/github/schema.graphql",
query_path = "src/github/create_repo.graphql",
response_derives = "Debug"
)]
pub struct CreateRepo;

pub fn create_repository(
    token: &str,
    template_data: &TemplateData,
    name: String,
) -> Result<String, GithubError> {
    let client = get_client();
    let variables = create_repo::Variables {
        repository_id: template_data.repo_id.clone(),
        name,
        owner_id: template_data.owner_id.clone(),
    };
    let request_body = CreateRepo::build_query(variables);
    let response = client
        .post("https://api.github.com/graphql")
        .set("Authorization", &format!("bearer {token}"))
        .send_json(request_body)?;
    let body: Response<create_repo::ResponseData> = response.into_json()?;
    println!("{body:?}");
    let output_name = body
        .data
        .and_then(|d| d.clone_template_repository)
        .and_then(|d| d.repository)
        .ok_or(GithubError::InvalidResponse)?
        .name_with_owner;

    Ok(output_name)
}

#[derive(Serialize)]
pub struct PutFile {
    message: String,
    content: String,
}

pub fn upload_index_file(
    token: &str,
    name_with_owner: &str,
    path: &Path,
) -> Result<(), GithubError> {
    let reader = BufReader::new(File::open(&path)?);
    let contents = io::read_to_string(reader)?;

    let put_file = PutFile {
        message: "Adding index".to_string(),
        content: general_purpose::STANDARD.encode(contents),
    };

    let client = get_client();
    let response = client
        .put(&format!("https://api.github.com/repos/{name_with_owner}/contents/index.json"))
        .set("Authorization", &format!("bearer {token}"))
        .set("X-GitHub-Api-Version", "2022-11-28")
        .set("Accept", "application/vnd.github+json")
        .set("Content-Type", "application/json").send_json(put_file)?;
    Ok(())
}
