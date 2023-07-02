mod archive;
mod data;
mod extract;
mod git;
mod github;
mod readme;
mod repository;

use crate::repository::index::RepositoryIndex;
use std::io;

use clap::{Parser, Subcommand};

use crate::extract::download_packages;
use crate::git::GitFastImporter;
use crate::github::index::get_repository_index;
use crate::github::projects::get_latest_pypi_data_repo;
use crate::github::release_data::download_pypi_data_release;
use crate::repository::package::RepositoryPackage;
use chrono::{DateTime, NaiveDateTime, Utc};
use git2::{BranchType, Repository};
use itertools::Itertools;
use rusqlite::Connection;
use std::path::PathBuf;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Split {
        sqlite_file: PathBuf,
        output_dir: PathBuf,

        #[clap(long)]
        latest_index: Option<PathBuf>,

        #[clap(short, long, default_value = "30000")]
        chunk_size: usize,

        #[clap(short, long, default_value = "100000")]
        limit: usize,
    },
    CreateRepository {
        index_path: PathBuf,

        #[clap(long, env)]
        github_token: String,
    },
    Extract {
        directory: PathBuf,

        #[clap(short, long)]
        limit: Option<usize>,

        #[clap(short, long)]
        index_file_name: String,
    },
    ShouldCiRun {
        repository_dir: PathBuf,
    },
    DownloadReleaseData {
        output: PathBuf,

        #[clap(long, env)]
        github_token: String,
    },
    FetchLatestIndex {
        #[clap(long, env)]
        github_token: String,
    },
    GenerateReadme {
        repository_dir: PathBuf,
    },
    Status {
        #[clap(long, env)]
        github_token: String,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Split {
            sqlite_file,
            latest_index,
            output_dir,
            chunk_size,
            limit,
        } => {
            let last_package_time = match latest_index {
                None => {
                    let zero_timestamp = NaiveDateTime::from_timestamp_opt(0, 0).unwrap();
                    DateTime::from_utc(zero_timestamp, Utc)
                }
                Some(index_path) => {
                    let idx = RepositoryIndex::from_path(&index_path)?;
                    idx.stats().latest_package
                }
            };

            let conn = Connection::open(&sqlite_file)?;
            let mut stmt = conn.prepare(
                "SELECT projects.name, \
                    projects.version, \
                    url, \
                    upload_time \
              FROM urls \
              join projects on urls.project_id = projects.id \
              where upload_time > ?1\
              order by upload_time ASC \
              LIMIT ?2",
            )?;
            let packages = stmt
                .query_map(rusqlite::params![last_package_time, limit], |row| {
                    Ok(RepositoryPackage {
                        project_name: row.get(0)?,
                        project_version: row.get(1)?,
                        url: row.get(2)?,
                        upload_time: row.get(3)?,
                        processed: false,
                    })
                })?
                .map(|v| v.unwrap());

            // let mut packages =
            //     data::get_ordered_packages_since(&sqlite_file, last_package_time, chunk_size, limit).unwrap();

            std::fs::create_dir_all(&output_dir)?;
            // if index.has_capacity() {
            //     index.fill_packages(&mut packages);
            //     index.to_file(&output_dir.join(latest_package_index.file_name().unwrap()))?;
            // }
            //
            // let mut max_repo_index = index.index();
            let mut max_repo_index = 0;

            for chunk_iter in &packages.chunks(chunk_size) {
                max_repo_index += 1;
                let chunk = chunk_iter.collect_vec();
                let new_index = RepositoryIndex::new(max_repo_index, chunk_size, &chunk);
                new_index.to_file(&output_dir.join(format!("{max_repo_index}.json")))?;
            }
        }
        Commands::CreateRepository {
            index_path,
            github_token,
        } => {
            let idx = RepositoryIndex::from_path(&index_path)?;
            let template_data = crate::github::create::get_template_data(&github_token)?;
            let result = crate::github::create::create_repository(
                &github_token,
                &template_data,
                format!("test-{}", idx.index()),
            )?;
            crate::github::create::upload_index_file(&github_token, &result, &index_path)?;
            println!("{template_data:#?} - {result}");
        }

        Commands::Extract {
            directory,
            limit,
            index_file_name,
        } => {
            let git_repo = Repository::open(&directory)?;
            let has_code_branch = git_repo
                .find_branch("code", BranchType::Local)
                .map(|_| true)
                .unwrap_or_default();
            let repo_index_file = directory.join("index.json");
            let repo_file_index_path = directory.join(index_file_name);
            let mut repo_index = RepositoryIndex::from_path(&repo_index_file)?;
            let mut unprocessed_packages = repo_index.unprocessed_packages();
            if let Some(limit) = limit {
                if limit < unprocessed_packages.len() {
                    unprocessed_packages.drain(limit..);
                }
            }
            let output = GitFastImporter::new(
                std::io::BufWriter::new(io::stdout()),
                unprocessed_packages.len(),
                "code".to_string(),
                has_code_branch,
            );
            let processed_packages =
                download_packages(unprocessed_packages, repo_file_index_path, output)?;

            repo_index.mark_packages_as_processed(processed_packages);
            repo_index.to_file(&repo_index_file)?;
        }
        Commands::DownloadReleaseData {
            output,
            github_token,
        } => {
            download_pypi_data_release(&github_token, &output, true)?;
        }
        Commands::FetchLatestIndex { github_token } => {
            let latest_repo_name = get_latest_pypi_data_repo(&github_token)?.unwrap();
            let index = get_repository_index(&github_token, &latest_repo_name, None)?;
            println!("index: {index}");
        }
        Commands::ShouldCiRun { repository_dir } => {
            let index = RepositoryIndex::from_path(&repository_dir.join("index.json"))?;
            let stats = index.stats();
            if stats.total_packages != stats.done_packages {
                println!("true");
            } else {
                println!("false");
            }
        }
        Commands::GenerateReadme { repository_dir } => {
            let index = RepositoryIndex::from_path(&repository_dir.join("index.json"))?;
            println!("{}", readme::generate_readme(index)?)
        }
        Commands::Status { github_token } => {
            let all_repos = github::projects::get_all_pypi_data_repos(&github_token)?;
            let client = crate::github::get_client();
            let indexes: Result<Vec<RepositoryIndex>, _> = all_repos
                .iter()
                .map(|name| {
                    github::index::get_repository_index(&github_token, name, Some(client.clone()))
                })
                .collect();
            let indexes = indexes?;
            let runs: Result<Vec<_>, _> = all_repos
                .iter()
                .map(|name| {
                    crate::github::workflows::get_workflow_runs(
                        &github_token,
                        name,
                        Some(client.clone()),
                    )
                })
                .collect();
            let runs = runs?;

            for (index, _runs) in indexes.iter().zip(runs) {
                let stats = index.stats();
                println!("Stats: {stats:?}: percent done: {}%", stats.percent_done());
            }

            // println!("Runs: {runs:#?}");
            // println!("Indexes: {indexes:?}");
        }
    }
    Ok(())
}
