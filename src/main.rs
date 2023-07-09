mod archive;
mod data;
mod extract;
mod git;
mod github;
mod readme;
mod repository;
mod stats;

use crate::repository::index::RepositoryIndex;
use clap::{Parser, Subcommand};
use std::fs::File;
use std::io;
use std::io::BufWriter;

use crate::extract::download_packages;
use crate::git::GitFastImporter;
use crate::repository::package::RepositoryPackage;
use chrono::{DateTime, NaiveDateTime, Utc};
use cli_table::{Cell, Style, Table};
use git2::{BranchType, Repository};
use itertools::Itertools;
use rand::thread_rng;

use crate::github::GithubError;
use humansize::DECIMAL;
use rand::seq::SliceRandom;
use rayon::prelude::*;
use rusqlite::Connection;
use std::path::PathBuf;
use std::thread::sleep;
use std::time::Duration;
use url::Url;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    #[clap(long)]
    tracing_file: Option<PathBuf>,
}

#[derive(Subcommand)]
enum Commands {
    // CI/trigger actions
    Extract {
        directory: PathBuf,

        #[clap(short, long)]
        limit: Option<usize>,

        #[clap(short, long)]
        filter_name: Option<String>,

        #[clap(short, long)]
        index_file_name: String,

        #[clap(short, long, default_value = "false")]
        skip_contents: bool,
    },
    GenerateReadme {
        repository_dir: PathBuf,
    },
    MergeParquet {
        output_file: PathBuf,

        index_files: Vec<PathBuf>,

        #[clap(short, long, default_value = "50000")]
        batch_size: usize,
    },

    // Creation/bootstrap commands
    CreateIndex {
        #[clap(short, long)]
        sqlite_file: PathBuf,

        #[clap(short, long)]
        input_dir: PathBuf,

        #[clap(short, long)]
        output_dir: PathBuf,

        #[clap(short, long, default_value = "40000")]
        chunk_size: usize,

        #[clap(short, long, default_value = "10")]
        limit: usize,

        #[clap(long, env)]
        after: Option<DateTime<Utc>>,
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
        #[clap(long, env)]
        github_token: String,
    },
    ListRepositoriesForTriggering {
        #[clap(long, short)]
        progress_less_than: usize,

        #[clap(long, short)]
        sample: usize,

        #[clap(long, env)]
        github_token: String,
    },
    DashboardJson {
        #[clap(long, env)]
        github_token: String,
    },
    GetAllIndexes {
        output_dir: PathBuf,

        #[clap(long, env)]
        github_token: String,
    },
    DebugPackage {
        url: Url,
    },
    DebugIndex {
        index_file_or_url: String,
        #[clap(short, long)]
        filter_name: Option<String>,
        #[clap(short, long, default_value = "false")]
        no_import: bool,

        #[clap(short, long, default_value = "false")]
        skip_contents: bool,
    },

    // Data commands
    ReduceParquet {
        input_dir: PathBuf,
        output_dir: PathBuf,
        #[clap(short, long, default_value = "500000")]
        batch_size: usize,
    },
    GenerateStatistics {
        input_dir: PathBuf,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let _guard = if let Some(tracing_file) = cli.tracing_file {
        let log_file = File::create(tracing_file)?;
        let (non_blocking, _guard) = tracing_appender::non_blocking(log_file);
        tracing_subscriber::fmt()
            .json()
            .with_writer(non_blocking)
            .init();
        Some(_guard)
    } else {
        None
    };

    match cli.command {
        // CI commands
        Commands::MergeParquet {
            output_file,
            index_files,
            batch_size,
        } => {
            data::merge_parquet_files(index_files, output_file, batch_size)?;
        }

        Commands::Extract {
            directory,
            limit,
            index_file_name,
            filter_name,
            skip_contents,
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
            if let Some(filter_name) = filter_name {
                unprocessed_packages.retain(|p| p.file_prefix().contains(&filter_name));
            }
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
                skip_contents,
            );
            let processed_packages = download_packages(
                unprocessed_packages,
                repo_file_index_path,
                output,
                repo_index.index(),
            )?;

            repo_index.mark_packages_as_processed(processed_packages);
            repo_index.to_file(&repo_index_file)?;
        }
        Commands::GenerateReadme { repository_dir } => {
            let index = RepositoryIndex::from_path(&repository_dir.join("index.json"))?;
            println!("{}", readme::generate_readme(index)?)
        }

        // Management commands:
        Commands::ListRepositoriesForTriggering {
            github_token,
            progress_less_than,
            sample,
        } => {
            let mut all_repos = github::projects::get_all_pypi_data_repos(&github_token)?;
            let mut rng = thread_rng();
            all_repos.shuffle(&mut rng);

            let client = github::get_client();
            let repos: Vec<_> = all_repos
                .into_iter()
                .flat_map(|repo| {
                    let index = github::index::get_repository_index(
                        &github_token,
                        &repo.name,
                        Some(client.clone()),
                    )?;
                    let stats = index.stats();
                    Ok::<(crate::github::projects::DataRepo, usize), GithubError>((
                        repo,
                        stats.percent_done(),
                    ))
                })
                .filter(|(_, percent_done)| *percent_done < progress_less_than)
                .take(sample)
                .collect();

            for (repo, _) in repos {
                println!("{}", repo.name);
            }
        }
        Commands::Status { github_token } => {
            let repo_status = github::status::get_status(&github_token, false)?;
            println!("{} repositories", repo_status.len());
            let mut table = vec![];
            for status in repo_status {
                table.push(vec![
                    status.name.cell(),
                    status.percent_done.cell(),
                    humansize::format_size(status.size, DECIMAL).cell(),
                ]);
            }
            let contents = table
                .table()
                .title(vec![
                    "Name".cell().bold(true),
                    "Progress".cell().bold(true),
                    "Repo Size".cell().bold(true),
                ])
                .display()
                .unwrap();
            println!("{contents}");
        }
        Commands::DashboardJson { github_token } => {
            let repo_status = github::status::get_status(&github_token, true)?;
            let _agent = ureq::agent();
            // let detailed_stats: Vec<_> = repo_status
            //     .into_par_iter()
            //     .map(|s| {
            //         let detailed = s.get_detailed_stats(agent.clone());
            //         (s, detailed)
            //     })
            //     .collect();
            println!("{}", serde_json::to_string_pretty(&repo_status)?);
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
            after,
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
            println!("Latest package time: {formatted_time}");

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
                if let Some(after) = after {
                    if chunk.iter().any(|p| p.upload_time < after) {
                        println!(
                            "Skipping chunk {} because it contains packages before {}",
                            max_repo_index, after
                        );
                        continue;
                    }
                }

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
                let stats = idx.stats();

                let result = github::create::create_repository(
                    &client,
                    &github_token,
                    &template_data,
                    idx.index(),
                    format!(
                        "Code uploaded to PyPi between {} and {}",
                        stats.earliest_package.format("%Y-%m-%d"),
                        stats.latest_package.format("%Y-%m-%d"),
                    ),
                )?;
                println!(
                    "Created repository for index: {}. Sleeping",
                    index_path.display()
                );
                sleep(Duration::from_secs(4));
                github::create::create_deploy_key(&client, &github_token, &result)?;
                github::index::upload_index_file(&client, &github_token, &result, &index_path)?;
                std::fs::copy(
                    &index_path,
                    output_dir.join(index_path.file_name().unwrap()),
                )?;
                println!(
                    "Finished creating repository for index: {}. Sleeping",
                    index_path.display()
                );
                sleep(Duration::from_secs(4));
            }
        }
        Commands::GetAllIndexes {
            output_dir,
            github_token,
        } => {
            std::fs::create_dir_all(&output_dir)?;
            let all_repos = github::projects::get_all_pypi_data_repos(&github_token)?;
            let client = github::get_client();
            all_repos.into_par_iter().for_each(|repo| {
                let output_path = output_dir.join(format!("{}.parquet", repo.name));
                let mut output_file =
                    std::io::BufWriter::new(std::fs::File::create(&output_path).unwrap());
                let url = format!(
                    "https://github.com/pypi-data/{}/releases/download/latest/combined.parquet",
                    repo.name
                );
                let response = client.get(&url).call();
                if let Ok(r) = response {
                    let mut reader = r.into_reader();
                    std::io::copy(&mut reader, &mut output_file).unwrap();
                    println!(
                        "Downloaded {} index to {}",
                        repo.name,
                        output_path.display()
                    );
                }
            });
        }
        Commands::DebugPackage { url } => {
            let out = std::io::stdout();
            let writer = GitFastImporter::new(
                std::io::BufWriter::new(out),
                1,
                "code".to_string(),
                true,
                true,
            );
            let agent = ureq::agent();
            let package = RepositoryPackage::fake_from_url(url);
            crate::extract::download_package(agent, &package, &writer).unwrap();
        }
        Commands::DebugIndex {
            index_file_or_url,
            filter_name,
            no_import,
            skip_contents,
        } => {
            let current_path = std::env::current_exe()?;
            let repository_dir = tempdir::TempDir::new("pypi-data")?;
            let tmp_path = repository_dir.into_path();
            Repository::init(&tmp_path)?;
            match url::Url::parse(&index_file_or_url) {
                Ok(v) => {
                    println!("Downloading from {v}");
                    let mut reader = ureq::request_url("GET", &v).call()?.into_reader();
                    let mut output =
                        BufWriter::new(std::fs::File::create(tmp_path.join("index.json"))?);
                    std::io::copy(&mut reader, &mut output)?;
                }
                Err(_) => {
                    println!("Copying from {index_file_or_url}");
                    std::fs::copy(index_file_or_url, tmp_path.join("index.json"))?;
                }
            }

            println!("Temporary repo created in {}", tmp_path.display());

            let mut args: Vec<String> = vec![
                "--tracing-file=tracing.txt",
                "extract",
                tmp_path.to_str().unwrap(),
                "--limit=15000",
                "--index-file-name=index.parquet",
            ]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
            if let Some(filter_name) = filter_name {
                args.push(format!("--filter-name={}", filter_name));
            }
            if skip_contents {
                args.push("--skip-contents".to_string());
            }

            if no_import {
                let stdout_file = File::create(tmp_path.join("log.txt"))?;
                duct::cmd(current_path, args)
                    .stdout_file(stdout_file)
                    .dir(&tmp_path)
                    .run()?;
            } else {
                duct::cmd(current_path, args)
                    // .pipe(duct::cmd!("tee", "log.txt").dir(&tmp_path))
                    .pipe(
                        duct::cmd!("git", "fast-import", format!("--max-pack-size=1G"))
                            .dir(&tmp_path),
                    )
                    .run()?;
            }
        }
        Commands::ReduceParquet {
            input_dir,
            output_dir,
            batch_size,
        } => {
            let input_files = std::fs::read_dir(&input_dir)?
                .flatten()
                .map(|e| e.path())
                .filter(|e| e.extension().unwrap_or_default() == "parquet")
                .collect::<Vec<_>>();
            crate::data::reduce_parquet_files(input_files, output_dir, batch_size)?;
        }
        Commands::GenerateStatistics { input_dir } => {
            crate::stats::count(&input_dir)?;
        }
    }
    Ok(())
}
