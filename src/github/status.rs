use crate::github;
use crate::github::workflows::WorkflowRun;
use crate::github::GithubError;
use crate::repository::index::RepoStats;

use rayon::prelude::*;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct RepoStatus {
    pub name: String,
    pub stats: RepoStats,
    pub idx: usize,
    pub percent_done: usize,
    pub size: u64,
    pub workflow_runs: Option<Vec<WorkflowRun>>,
}

pub fn get_status(github_token: &str, with_runs: bool) -> Result<Vec<RepoStatus>, GithubError> {
    let all_repos = github::projects::get_all_pypi_data_repos(github_token)?;
    let client = github::get_client();
    let indexes: Result<Vec<RepoStatus>, GithubError> = all_repos
        .into_par_iter()
        .map(|repo| {
            let index = github::index::get_repository_index(
                github_token,
                &repo.name,
                Some(client.clone()),
            )?;
            let workflow_runs = if with_runs {
                let runs = github::workflows::get_workflow_runs(
                    github_token,
                    &repo.name,
                    Some(client.clone()),
                    3,
                )?;
                Some(runs.workflow_runs)
            } else {
                None
            };
            let stats = index.stats();
            let status = RepoStatus {
                name: repo.name,
                percent_done: stats.percent_done(),
                stats,
                workflow_runs,
                idx: index.index(),
                size: (repo.size * 1024) as u64,
            };
            Ok(status)
        })
        .collect();

    indexes
}
