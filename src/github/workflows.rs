use anyhow::Context;
use serde::Deserialize;
use crate::github::{get_client, GithubError};
use crate::repository::index::RepositoryIndex;
use ureq::Agent;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Completed,
    ActionRequired,
    Cancelled,
    Failure,
    Neutral,
    Skipped,
    Stale,
    Success,
    TimedOut,
    InProgress,
    Queued,
    Requested,
    Waiting,
    Pending
}

#[derive(Deserialize, Debug)]
pub struct WorkflowRun {
    html_url: String,
    status: Status,
    conclusion: Option<Status>
}

#[derive(Deserialize, Debug)]
pub struct WorkflowRuns {
    workflow_runs: Vec<WorkflowRun>
}

pub fn get_workflow_runs(token: &str, name: &str, client: Option<Agent>) -> Result<WorkflowRuns, GithubError> {
    let client = client.unwrap_or_else(|| get_client());

    let response = client
        .get(&format!("https://api.github.com/repos/pypi-data/{name}/actions/workflows/test.yml/runs?branch=main&exclude_pull_requests=true"))
        .set("Authorization", &format!("bearer {token}"))
        .set("X-GitHub-Api-Version", "2022-11-28")
        .set("Accept", "application/vnd.github+json").call()?;
    // return
    // println!("{}", response.into_string()?);
    // return Ok(());
    Ok(serde_json::from_str(&response.into_string()?).with_context(|| format!("Error getting index content for {name}"))?)
}
