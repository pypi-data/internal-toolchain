mod archive;
mod data;
mod extract;
mod git;
mod github;
mod readme;
mod repository;

use crate::repository::index::RepositoryIndex;
use clap::{Parser, Subcommand};
use std::io;

use crate::extract::download_packages;
use crate::git::GitFastImporter;
use crate::repository::package::RepositoryPackage;
use chrono::{DateTime, NaiveDateTime, Utc};
use git2::{BranchType, Repository};
use itertools::Itertools;

use rusqlite::Connection;
use std::path::PathBuf;
use std::thread::sleep;
use std::time::Duration;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    // CI/trigger actions
    Extract {
        directory: PathBuf,

        #[clap(short, long)]
        limit: Option<usize>,

        #[clap(short, long)]
        index_file_name: String,
    },
    GenerateReadme {
        repository_dir: PathBuf,
    },
    MergeParquet {
        output_file: PathBuf,

        index_files: Vec<PathBuf>,
    },

    // Creation/bootstrap commands
    CreateIndex {
        #[clap(short, long)]
        sqlite_file: PathBuf,

        #[clap(short, long)]
        input_dir: PathBuf,

        #[clap(short, long)]
        output_dir: PathBuf,

        #[clap(short, long, default_value = "30000")]
        chunk_size: usize,

        #[clap(short, long, default_value = "10")]
        limit: usize,
    },
    CreateRepositories {
        output_dir: PathBuf,

        index_paths: Vec<PathBuf>,

        #[clap(long, env)]
        github_token: String,
    },

    // Status/trigger commands
    TriggerCi {
        name: String,

        #[clap(long, short)]
        limit: usize,

        #[clap(long, env)]
        github_token: String,
    },
    Status {
        #[clap(long, short, default_value="20")]
        progress_less_than: usize,

        #[clap(long, env)]
        github_token: String,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        // CI commands
        Commands::MergeParquet {
            output_file,
            index_files,
        } => {
            data::merge_parquet_files(index_files, output_file);
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
        Commands::GenerateReadme { repository_dir } => {
            let index = RepositoryIndex::from_path(&repository_dir.join("index.json"))?;
            println!("{}", readme::generate_readme(index)?)
        }

        // Management commands:
        Commands::Status { github_token, progress_less_than } => {
            let all_repos = github::projects::get_all_pypi_data_repos(&github_token)?;
            let client = github::get_client();
            let indexes: Result<Vec<(_, _)>, _> = all_repos
                .iter()
                .map(|name| {
                    github::index::get_repository_index(&github_token, name, Some(client.clone())).map(|r| (name, r))
                })
                .collect();
            let indexes = indexes?;
            // let runs: Result<Vec<_>, _> = all_repos
            //     .iter()
            //     .map(|name| {
            //         github::workflows::get_workflow_runs(&github_token, name, Some(client.clone()))
            //     })
            //     .collect();
            // let runs = runs?;

            for (name, index) in indexes {
                let stats = index.stats();
                if stats.percent_done() < progress_less_than {
                    println!("{name}")
                }
                // println!("Stats: {stats:?}: percent done: {}%", stats.percent_done());
            }
        }
        Commands::TriggerCi {
            name,
            github_token,
            limit,
        } => {
            github::trigger_ci::trigger_ci_workflow(
                &github_token,
                &format!("pypi-data/{name}"),
                limit,
            )?;
        }

        Commands::CreateIndex {
            sqlite_file,
            input_dir,
            output_dir,
            chunk_size,
            limit,
        } => {
            std::fs::create_dir_all(&output_dir)?;

            let max_index_file = std::fs::read_dir(&input_dir)?
                .flatten()
                .filter_map(|entry| {
                    if entry.file_type().ok()?.is_file() {
                        let path = entry.path();
                        let path_str = path.file_name()?.to_str()?;
                        let first_component = path_str.split('.').next()?;
                        let index = first_component.parse::<usize>().ok()?;
                        Some((index, entry))
                    } else {
                        None
                    }
                })
                .max_by(|(i1, _), (i2, _)| i1.cmp(i2));
            let (latest_package_time, latest_package) = match max_index_file {
                None => {
                    let zero_timestamp = NaiveDateTime::from_timestamp_opt(0, 0).unwrap();
                    (DateTime::from_utc(zero_timestamp, Utc), None)
                }
                Some((_, dir_entry)) => {
                    let idx = RepositoryIndex::from_path(&dir_entry.path()).unwrap();
                    let latest_package = idx.stats().latest_package;
                    println!("Using latest package time from index: {}.", idx.index());
                    (latest_package, Some(idx))
                }
            };

            let formatted_time = format!("{latest_package_time:?}");

            let conn = Connection::open(&sqlite_file)?;
            let mut stmt = conn.prepare(
                "SELECT projects.name, \
                    projects.version, \
                    url, \
                    upload_time \
              FROM urls \
              join projects on urls.project_id = projects.id \
              where upload_time > ?1\
              order by upload_time ASC",
            )?;
            let mut packages = stmt
                .query_map([formatted_time], |row| {
                    Ok(RepositoryPackage {
                        project_name: row.get(0)?,
                        project_version: row.get(1)?,
                        url: row.get(2)?,
                        upload_time: row.get(3)?,
                        processed: false,
                    })
                })?
                .map(|v| v.unwrap());

            let mut max_repo_index = if let Some(mut index) = latest_package {
                if index.has_capacity() {
                    let mut extra_capacity = index.extra_capacity();
                    let mut collector = vec![];
                    while extra_capacity > 0 {
                        if let Some(package) = packages.next() {
                            println!("Expanding {package}");
                            collector.push(package);
                            extra_capacity -= 1;
                        } else {
                            break;
                        }
                    }
                    if !collector.is_empty() {
                        let new_package_len = collector.len();
                        index.fill_packages(collector);
                        index.to_file(&output_dir.join(index.file_name()))?;
                        println!(
                            "Updated last index {} with {} packages. Extra capacity: {}",
                            index.file_name(),
                            new_package_len,
                            index.extra_capacity()
                        );
                    }
                }
                index.index()
            } else {
                0
            };

            for chunk_iter in packages.chunks(chunk_size).into_iter().take(limit) {
                max_repo_index += 1;
                let chunk = chunk_iter.collect_vec();
                let new_index = RepositoryIndex::new(max_repo_index, chunk_size, &chunk);
                new_index.to_file(&output_dir.join(new_index.file_name()))?;
                println!("Created index {}", new_index.file_name());
            }
        }
        Commands::CreateRepositories {
            output_dir,
            index_paths,
            github_token,
        } => {
            std::fs::create_dir_all(&output_dir)?;
            let client = github::get_client();
            let template_data = github::create::get_template_data(&client, &github_token)?;

            for index_path in index_paths {
                println!("Creating repository for index: {}", index_path.display());
                let idx = RepositoryIndex::from_path(&index_path)?;
                let result = github::create::create_repository(
                    &client,
                    &github_token,
                    &template_data,
                    format!("pypi-code-{}", idx.index()),
                )?;
                println!(
                    "Created repository for index: {}. Sleeping for 10 seconds",
                    index_path.display()
                );
                sleep(Duration::from_secs(10));
                github::create::create_deploy_key(&client, &github_token, &result)?;
                github::index::upload_index_file(&client, &github_token, &result, &index_path)?;
                std::fs::copy(
                    &index_path,
                    output_dir.join(index_path.file_name().unwrap()),
                )?;
                println!(
                    "Finished creating repository for index: {}. Sleeping for 10 seconds",
                    index_path.display()
                );
                sleep(Duration::from_secs(10));
            }
        }
    }
    Ok(())
}
