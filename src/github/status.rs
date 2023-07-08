use crate::github;
use crate::github::workflows::WorkflowRun;
use crate::github::GithubError;
use crate::repository::index::RepoStats;

#[cfg(feature = "stats")]
use polars::prelude::*;

use rayon::prelude::*;
use serde::Serialize;

use ureq::Agent;
use url::Url;

#[derive(Debug, Serialize)]
pub struct RepoStatus {
    pub name: String,
    pub stats: RepoStats,
    pub percent_done: usize,
    pub size_kb: u64,
    pub workflow_runs: Option<Vec<WorkflowRun>>,
}

#[derive(Debug, Serialize)]
pub struct DetailedStats {
    pub total_size: u64,
    pub top_projects: Vec<ProjectStat>,
}

#[derive(Debug, Serialize)]
pub struct ProjectStat {
    name: String,
    total_size: u64,
    text_size: u64,
    total_files: u64,
    unique_files: u64,
}

impl RepoStatus {
    pub fn parquet_url(&self) -> Url {
        format!(
            "https://github.com/pypi-data/{}/releases/download/latest/combined.parquet",
            self.name
        )
        .parse()
        .unwrap()
    }
    #[cfg(not(feature = "stats"))]
    pub fn get_detailed_stats(&self, _client: Agent) -> DetailedStats {
        panic!("stats feature not enabled");
    }

    #[cfg(feature = "stats")]
    pub fn get_detailed_stats(&self, client: Agent) -> DetailedStats {
        let tmp_dir = tempdir::TempDir::new("status").unwrap();
        let parquet_path = tmp_dir.path().join("combined.parquet");
        let request = client.get(self.parquet_url().as_ref()).call().unwrap();
        let mut reader = request.into_reader();
        let mut output = BufWriter::new(File::create(&parquet_path).unwrap());
        std::io::copy(&mut reader, &mut output).unwrap();

        let df = LazyFrame::scan_parquet(&parquet_path, Default::default()).unwrap();
        let binding = df
            .groupby([col("project_name")])
            .agg([
                col("size").sum().alias("size").cast(DataType::UInt64),
                col("size")
                    .filter(col("content_type").eq(lit("text")))
                    .sum()
                    .alias("text_size")
                    .cast(DataType::UInt64),
                col("hash")
                    .count()
                    .alias("total_files")
                    .cast(DataType::UInt64),
                col("hash")
                    .unique()
                    .count()
                    .alias("unique_files")
                    .cast(DataType::UInt64),
            ])
            .sort(
                "text_size",
                SortOptions {
                    descending: true,
                    nulls_last: true,
                    multithreaded: true,
                },
            )
            .limit(5)
            .collect()
            .unwrap();
        let df2 = LazyFrame::scan_parquet(&parquet_path, Default::default()).unwrap();
        let binding2 = df2
            .select(&[
                sum("size").alias("total_size").cast(DataType::UInt64),
                // col("hash")
                //     .sort_by([col("size").rank(Default::default(), None)], [false])
                //     .last()
                //     .alias("largest_file"),
            ])
            .collect()
            .unwrap();

        let total_size = binding2.select_series(["total_size"]).unwrap()[0]
            .u64()
            .unwrap()
            .get(0)
            .unwrap();

        // let x = binding.get_row(0).unwrap();
        // println!("{x:?}");
        let mut top_projects = vec![];
        let series = binding
            .select_series([
                "project_name",
                "size",
                "text_size",
                "total_files",
                "unique_files",
            ])
            .unwrap();
        for (name, size, text_size, total_files, unique_files) in itertools::multizip((
            series[0].utf8().unwrap(),
            series[1].u64().unwrap(),
            series[2].u64().unwrap(),
            series[3].u64().unwrap(),
            series[4].u64().unwrap(),
        )) {
            top_projects.push(ProjectStat {
                name: name.unwrap().to_string(),
                total_size: size.unwrap(),
                // total_size_human: format_size(size.unwrap(), DECIMAL),
                text_size: text_size.unwrap(),
                // text_size_human: format_size(text_size.unwrap(), DECIMAL),
                total_files: total_files.unwrap(),
                unique_files: unique_files.unwrap(),
            });
            // println!("{name:?} {size:?} {total_files:?} {unique_files:?}");
        }
        // let x = binding.get_row(0).unwrap();
        // println!("{x:?}");
        // let foo = match x.0.get(0).unwrap() {
        //     AnyValue::UInt64(v) => { *v }
        //     _ => unreachable!(),
        // };

        DetailedStats {
            total_size,
            // total_size_human: format_size(total_size, DECIMAL),
            top_projects,
        }
    }
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
                    5,
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
                size_kb: repo.size as u64,
            };
            Ok(status)
        })
        .collect();

    indexes
}
