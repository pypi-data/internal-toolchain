use crate::github;
use crate::github::workflows::WorkflowRun;
use crate::github::GithubError;
use crate::repository::index::{RepoStats, RepositoryIndex};
use indicatif::ParallelProgressIterator;

use rayon::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct RepoStatus {
    pub name: String,
    pub stats: RepoStats,
    pub idx: usize,
    pub percent_done: usize,
    pub size: u64,
    pub workflow_runs: Option<Vec<WorkflowRun>>,
    pub index: RepositoryIndex,
}

pub fn get_status(
    github_token: &str,
    with_runs: bool,
    limit: Option<usize>,
) -> Result<Vec<RepoStatus>, GithubError> {
    let all_repos = github::projects::get_all_pypi_data_repos(github_token)?;
    let client = github::get_client();
    let limit = limit.unwrap_or(all_repos.len());
    let indexes: Result<Vec<RepoStatus>, GithubError> = all_repos
        .into_par_iter()
        .take(limit)
        .progress()
        .map(|repo| {
            let index = github::index::get_repository_index(&repo.name, Some(client.clone()))?;
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
                index,
            };
            Ok(status)
        })
        .collect();

    indexes
}
