use std::fs::File;
use std::io::BufWriter;
use itertools::Itertools;
use rayon::iter::IntoParallelIterator;
use crate::github;
use crate::github::GithubError;
use serde::Serialize;
use ureq::Agent;
use url::Url;
use crate::github::workflows::WorkflowRun;
use crate::repository::index::RepoStats;
use polars::prelude::*;

#[derive(Debug, Serialize)]
pub struct RepoStatus {
    pub name: String,
    pub stats: RepoStats,
    pub percent_done: usize,
    pub workflow_runs: Option<Vec<WorkflowRun>>
}

impl RepoStatus {
    pub fn parquet_url(&self) -> Url {
        format!("https://github.com/pypi-data/{}/releases/download/latest/combined.parquet", self.name).parse().unwrap()
    }

    pub fn get_detailed_stats(&self, client: Agent) {
        let tmp_dir = tempdir::TempDir::new("status").unwrap();
        let parquet_path = tmp_dir.path().join("combined.parquet");
        let request = client.get(&self.parquet_url().to_string()).call().unwrap();
        let mut reader = request.into_reader();
        let mut output = BufWriter::new(File::create(&parquet_path).unwrap());
        std::io::copy(&mut reader, &mut output).unwrap();

        let df = LazyFrame::scan_parquet(&parquet_path, Default::default()).unwrap();
        let binding = df.select(
            &[sum("size").alias("total_size")]
        ).collect().unwrap();
        let x = binding.get_row(0).unwrap();
        println!("{x:?}");
    }
}


pub fn get_status(github_token: &str, with_runs: bool) -> Result<Vec<RepoStatus>, GithubError> {
    let all_repos = github::projects::get_all_pypi_data_repos(&github_token)?;
    let client = github::get_client();
    let indexes: Result<Vec<(_, _)>, _> = all_repos
        .iter()
        .map(|name| {
            github::index::get_repository_index(&github_token, name, Some(client.clone()))
                .map(|r| (name, r))
        })
        .collect();
    let indexes = indexes?.into_iter().map(|(name,index)| {
        let workflow_runs = if with_runs {
            let runs = github::workflows::get_workflow_runs(&github_token, &name, Some(client.clone()), 5).unwrap();
            Some(runs.workflow_runs)
        } else {
            None
        };
        let stats = index.stats();
        let status = RepoStatus {
            name: name.clone(),
            percent_done: stats.percent_done(),
            stats,
            workflow_runs
        };
        status
    }).collect_vec();

    return Ok(indexes);
    // let runs: Result<Vec<_>, _> = all_repos
    //     .iter()
    //     .map(|name| {
    //         github::workflows::get_workflow_runs(&github_token, name, Some(client.clone()))
    //     })
    //     .collect();
    // let runs = runs?;
}
