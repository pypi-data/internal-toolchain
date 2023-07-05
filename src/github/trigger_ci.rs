use crate::github::{get_client, GithubError};
use serde::Serialize;

#[derive(Serialize)]
pub struct TriggerWorkflowInputs {
    limit: String,
}

#[derive(Serialize)]
pub struct TriggerWorkflow {
    #[serde(rename = "ref")]
    ref_: String,
    inputs: TriggerWorkflowInputs,
}

pub fn trigger_ci_workflow(
    token: &str,
    name_with_owner: &str,
    limit: usize,
) -> Result<(), GithubError> {
    let client = get_client();
    client
        .post(&format!(
            "https://api.github.com/repos/{name_with_owner}/actions/workflows/trigger.yml/dispatches"
        ))
        .set("Authorization", &format!("bearer {token}"))
        .set("X-GitHub-Api-Version", "2022-11-28")
        .set("Accept", "application/vnd.github.v3+json")
        .set("Content-Type", "application/json")
        .send_json(TriggerWorkflow { ref_: "main".to_string(), inputs: TriggerWorkflowInputs { limit: limit.to_string() } }).map_err(Box::new)?;

    Ok(())
}
