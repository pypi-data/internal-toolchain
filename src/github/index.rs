use crate::github::{get_client, GithubError};
use crate::repository::index::RepositoryIndex;
use graphql_client::{GraphQLQuery, Response};

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "src/github/schema.graphql",
    query_path = "src/github/get_index.graphql",
    response_derives = "Debug"
)]
pub struct GetIndex;

pub fn get_repository_index(token: &str, name: &str) -> Result<RepositoryIndex, GithubError> {
    let client = get_client();
    let variables = get_index::Variables {
        name: name.to_string(),
    };

    let request_body = GetIndex::build_query(variables);
    let response = client
        .post("https://api.github.com/graphql")
        .set("Authorization", &format!("bearer {token}"))
        .send_json(request_body)?;

    let body: Response<get_index::ResponseData> = response.into_json()?;
    let content: String = body
        .data
        .and_then(|b| b.repository)
        .and_then(|o| o.object)
        .and_then(|o| match o {
            get_index::GetIndexRepositoryObject::Blob(b) => Some(b),
            _ => None,
        })
        .and_then(|b| b.text)
        .ok_or(GithubError::InvalidResponse)?;

    Ok(serde_json::from_str(&content)?)
}
