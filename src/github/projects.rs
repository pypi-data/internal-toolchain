use crate::github::{get_client, GithubError};
use graphql_client::{GraphQLQuery, Response};

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "src/github/schema.graphql",
    query_path = "src/github/list_projects.graphql",
    response_derives = "Debug"
)]
pub struct ListProjects;

const REPO_CODE_PREFIX: &str = "pypi-code-new-";

pub fn get_all_pypi_data_repos(token: &str) -> Result<Vec<String>, GithubError> {
    let client = get_client();
    let mut cursor = None;
    let mut repo_names = vec![];
    loop {
        let variables = list_projects::Variables {
            cursor: cursor.clone(),
        };
        let request_body = ListProjects::build_query(variables);
        let response = client
            .post("https://api.github.com/graphql")
            .set("Authorization", &format!("bearer {token}"))
            .send_json(request_body)?;
        let body: Response<list_projects::ResponseData> = response.into_json()?;
        let repositories = body
            .data
            .and_then(|b| b.repository_owner)
            .map(|o| o.repositories)
            .ok_or(GithubError::InvalidResponse)?;
        let has_next_page = repositories.page_info.has_next_page;
        cursor = repositories.page_info.end_cursor;
        let nodes = repositories.nodes.ok_or(GithubError::InvalidResponse)?;

        repo_names.extend(
            nodes
                .into_iter()
                .flat_map(|n| n.map(|r| r.name))
                .filter(|n| n.starts_with(REPO_CODE_PREFIX)),
        );

        if !has_next_page {
            break;
        }
    }
    Ok(repo_names)
}

pub fn get_latest_pypi_data_repo(token: &str) -> Result<Option<String>, GithubError> {
    let repo_names = get_all_pypi_data_repos(&token)?;
    let max_repo = repo_names
        .into_iter()
        .flat_map(|name| match name.rsplit_once('-') {
            None => None,
            Some((_, right)) => match right.parse::<usize>() {
                Ok(integer) => Some((name, integer)),
                Err(_) => None,
            },
        })
        .max_by_key(|(_, int)| *int);
    match max_repo {
        None => Ok(None),
        Some((name, _)) => Ok(Some(name)),
    }
}
