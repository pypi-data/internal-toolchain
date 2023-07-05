use crate::github::{get_client, GithubError};
use graphql_client::{GraphQLQuery, Response};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io;
use std::io::BufReader;
use std::path::Path;

use base64::{engine::general_purpose, Engine as _};
use osshkeys::cipher::Cipher;
use sodiumoxide::crypto;
use sodiumoxide::crypto::secretbox::seal;
use ureq::{Agent, Error};

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "src/github/graphql/schema.graphql",
    query_path = "src/github/graphql/get_template_data.graphql",
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
    schema_path = "src/github/graphql/schema.graphql",
    query_path = "src/github/graphql/create_repo.graphql",
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
    client
        .put(&format!(
            "https://api.github.com/repos/{name_with_owner}/contents/index.json"
        ))
        .set("Authorization", &format!("bearer {token}"))
        .set("X-GitHub-Api-Version", "2022-11-28")
        .set("Accept", "application/vnd.github+json")
        .set("Content-Type", "application/json")
        .send_json(put_file)?;
    Ok(())
}

#[derive(Serialize)]
pub struct CreateDeployKey {
    title: String,
    key: String,
    read_only: bool,
}

pub fn create_deploy_key(token: &str, name_with_owner: &str) -> Result<(), GithubError> {
    let client = get_client();
    let keypair = osshkeys::KeyPair::generate(osshkeys::KeyType::ED25519, 256).unwrap();
    let pub_key = keypair.serialize_publickey().unwrap();
    let private_key = keypair.serialize_openssh(None, Cipher::Null).unwrap();

    let (public_key, key_id) = get_repo_public_key(&client, token, name_with_owner)?;

    use sodiumoxide::crypto::box_::curve25519xsalsa20poly1305::PublicKey;
    use sodiumoxide::crypto::sealedbox::curve25519blake2bxsalsa20poly1305::seal;
    let key = PublicKey::from_slice(&public_key).unwrap();
    let sealed_box = seal(private_key.as_bytes(), &key);
    let contents = general_purpose::STANDARD.encode(sealed_box);

    create_actions_secret(&client, token, name_with_owner, contents, key_id)?;

    let create_deploy_key = CreateDeployKey {
        title: "Auto-generated deploy key".to_string(),
        key: pub_key,
        read_only: false,
    };

    let res = client
        .post(&format!(
            "https://api.github.com/repos/{name_with_owner}/keys"
        ))
        .set("Authorization", &format!("bearer {token}"))
        .set("X-GitHub-Api-Version", "2022-11-28")
        .set("Accept", "application/vnd.github+json")
        .set("Content-Type", "application/json")
        .send_json(&create_deploy_key);
    match res {
        Ok(response) => { /* it worked */ }
        Err(Error::Status(code, response)) => {
            /* the server returned an unexpected status
            code (such as 400, 500 etc) */
            panic!("{}: {}", code, response.into_string().unwrap());
        }
        Err(_) => { /* some kind of io/transport error */ }
    }
    Ok(())
}

#[derive(Deserialize)]
pub struct RepoPublicKey {
    key: String,
    key_id: String,
}

fn get_repo_public_key(
    client: &Agent,
    token: &str,
    name_with_owner: &str,
) -> Result<(Vec<u8>, String), GithubError> {
    let res = client
        .get(&format!(
            "https://api.github.com/repos/{name_with_owner}/actions/secrets/public-key"
        ))
        .set("Authorization", &format!("bearer {token}"))
        .set("X-GitHub-Api-Version", "2022-11-28")
        .set("Accept", "application/vnd.github+json")
        .call()?;
    let output: RepoPublicKey = res.into_json()?;

    Ok((
        general_purpose::STANDARD.decode(output.key).unwrap(),
        output.key_id,
    ))
}

#[derive(Serialize)]
pub struct CreateSecret {
    encrypted_value: String,
    key_id: String,
}

fn create_actions_secret(
    client: &Agent,
    token: &str,
    name_with_owner: &str,
    encrypted_value: String,
    key_id: String,
) -> Result<(), GithubError> {
    let res = client
        .put(&format!(
            "https://api.github.com/repos/{name_with_owner}/actions/secrets/DEPLOY_KEY"
        ))
        .set("Authorization", &format!("bearer {token}"))
        .set("X-GitHub-Api-Version", "2022-11-28")
        .set("Accept", "application/vnd.github+json")
        .send_json(CreateSecret {
            encrypted_value,
            key_id,
        });

    match res {
        Ok(response) => { /* it worked */ }
        Err(Error::Status(code, response)) => {
            /* the server returned an unexpected status
            code (such as 400, 500 etc) */
            panic!("{}: {}", code, response.into_string().unwrap());
        }
        Err(_) => { /* some kind of io/transport error */ }
    }

    Ok(())
}