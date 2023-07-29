use crate::github::create::REPO_CODE_PREFIX;
use crate::github::{get_client, GithubError};
use graphql_client::{GraphQLQuery, Response};

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "src/github/graphql/schema.graphql",
    query_path = "src/github/graphql/list_projects.graphql",
    response_derives = "Debug"
)]
pub struct ListProjects;

#[derive(Debug)]
pub struct DataRepo {
    pub name: String,
    pub size: i64,
}

impl DataRepo {
    pub fn repo_index_integer(&self) -> usize {
        let without_name = self.name.replace("pypi-mirror-", "");
        without_name.parse().unwrap()
    }
}

pub fn get_all_pypi_data_repos(token: &str) -> Result<Vec<DataRepo>, GithubError> {
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
            .send_json(request_body)
            .map_err(Box::new)?;
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
                .flat_map(|n| n.map(|r| (r.name, r.disk_usage.unwrap_or_default())))
                .filter(|n| n.0.starts_with(REPO_CODE_PREFIX))
                .map(|(name, size)| DataRepo { name, size }),
        );

        if !has_next_page {
            break;
        }
    }
    Ok(repo_names)
}
