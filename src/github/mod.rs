use std::io;
use thiserror::Error;

pub mod create;
pub mod index;
pub mod projects;
pub mod release_data;
pub mod workflows;

#[derive(Error, Debug)]
pub enum GithubError {
    #[error("Malformed GraphQL response received")]
    InvalidResponse,

    #[error("Request error: {0}")]
    RequestError(#[from] ureq::Error),

    #[error("IO Error: {0}")]
    IOError(#[from] io::Error),

    #[error("Serde error: {0}")]
    SerdeError(#[from] anyhow::Error),
}

pub fn get_client() -> ureq::Agent {
    ureq::AgentBuilder::new()
        .user_agent("pypi-data/toolchain")
        .build()
}
