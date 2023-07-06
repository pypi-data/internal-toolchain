use itertools::Itertools;
use crate::github;
use crate::github::GithubError;
use rand::seq::IteratorRandom;
use rand::thread_rng;
use serde::Serialize;
use crate::repository::index::RepoStats;

#[derive(Debug, Serialize)]
pub struct RepoStatus {
    pub name: String,
    pub stats: RepoStats,
    pub percent_done: usize
}



pub fn get_status(github_token: &str) -> Result<Vec<RepoStatus>, GithubError> {
    let all_repos = github::projects::get_all_pypi_data_repos(&github_token)?;
    let client = github::get_client();
    let indexes: Result<Vec<(_, _)>, _> = all_repos
        .iter()
        .map(|name| {
            github::index::get_repository_index(&github_token, name, Some(client.clone()))
                .map(|r| (name, r))
        })
        .collect();
    let indexes = indexes?.into_iter().map(|(name,index)| RepoStatus {
        name: name.clone(),
        stats: index.stats(),
        percent_done: index.stats().percent_done()
    }).collect_vec();
    return Ok(indexes);
    // let runs: Result<Vec<_>, _> = all_repos
    //     .iter()
    //     .map(|name| {
    //         github::workflows::get_workflow_runs(&github_token, name, Some(client.clone()))
    //     })
    //     .collect();
    // let runs = runs?;


    //
    //
    //
    // Ok(items)
}
