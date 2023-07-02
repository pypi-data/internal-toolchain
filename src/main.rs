mod archive;
mod data;
mod extract;
mod git;
mod github;
mod readme;
mod repository;

use crate::repository::index::RepositoryIndex;

use clap::{Parser, Subcommand};

use crate::extract::download_packages;
use crate::github::index::get_repository_index;
use crate::github::projects::get_latest_pypi_data_repo;
use crate::github::release_data::download_pypi_data_release;
use duct::cmd;
use git2::Repository;
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
        latest_package_index: PathBuf,
        output_dir: PathBuf,

        #[clap(short, long, default_value = "30000")]
        chunk_size: usize,

        #[clap(short, long)]
        limit: Option<usize>,
    },
    BootstrapRepo {
        repo_index: PathBuf,
        output_dir: PathBuf,
    },
    Extract {
        directory: PathBuf,

        #[clap(short, long)]
        limit: Option<usize>,

        #[clap(short, long)]
        index_file_name: String,
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
    Ci {
        repository_dir: PathBuf,
        code_dir: PathBuf,

        #[clap(short, long, default_value = "700M")]
        pack_size: String,

        #[clap(short, long)]
        limit: Option<usize>,

        #[clap(short, long)]
        index_file_name: String,
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
            latest_package_index,
            output_dir,
            chunk_size,
            limit,
        } => {
            let mut index = RepositoryIndex::from_path(&latest_package_index)?;
            let last_package_time = index.stats().latest_package;
            let mut packages =
                data::get_ordered_packages_since(&sqlite_file, last_package_time, limit).unwrap();

            if index.has_capacity() {
                index.fill_packages(&mut packages);
                index.to_file(&output_dir.join(latest_package_index.file_name().unwrap()))?;
            }

            let mut max_repo_index = index.index();

            for chunk in packages.chunks(chunk_size) {
                max_repo_index += 1;
                let new_index = RepositoryIndex::new(max_repo_index, chunk_size, chunk);
                new_index.to_file(&output_dir.join(format!("{max_repo_index}.json")))?;
            }
        }

        Commands::BootstrapRepo {
            repo_index,
            output_dir,
        } => {
            let repo_index = RepositoryIndex::from_path(&repo_index)?;
            std::fs::create_dir_all(&output_dir)?;
            repo_index.to_file(&output_dir.join("index.json"))?;
            Repository::init(&output_dir)?;
        }

        Commands::Extract {
            directory,
            limit,
            index_file_name,
        } => {
            let repo_index_file = directory.join("index.json");
            let repo_file_index_path = directory.join(index_file_name);
            let mut repo_index = RepositoryIndex::from_path(&repo_index_file)?;
            let mut unprocessed_packages = repo_index.unprocessed_packages();
            if let Some(limit) = limit {
                if limit < unprocessed_packages.len() {
                    unprocessed_packages.drain(limit..);
                }
            }
            let processed_packages = download_packages(unprocessed_packages, repo_file_index_path)?;

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
        Commands::Ci {
            repository_dir,
            code_dir,
            pack_size,
            limit,
            index_file_name,
        } => {
            let current_path = std::env::current_exe()?;
            let limit = match limit {
                None => "".to_string(),
                Some(l) => format!("--limit={l}"),
            };
            let index_file_name = format!("--index-file-name={index_file_name}");
            cmd!(
                &current_path,
                "extract",
                &repository_dir,
                limit,
                index_file_name
            )
            .pipe(
                cmd!(
                    "git",
                    "fast-import",
                    "--force",
                    format!("--max-pack-size={pack_size}")
                )
                .dir(code_dir),
            )
            .start()?
            .wait()?;
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
