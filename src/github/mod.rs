use std::io;
use thiserror::Error;

pub mod index;
pub mod projects;
pub mod release_data;

#[derive(Error, Debug)]
pub enum GithubError {
    #[error("Malformed GraphQL response received")]
    InvalidResponse,

    #[error("Request error: {0}")]
    RequestError(#[from] ureq::Error),

    #[error("IO Error: {0}")]
    IOError(#[from] io::Error),

    #[error("Serde error: {0}")]
    SerdeError(#[from] serde_json::Error),
}

fn get_client() -> ureq::Agent {
    ureq::AgentBuilder::new()
        .user_agent("pypi-data/toolchain")
        .build()
}
