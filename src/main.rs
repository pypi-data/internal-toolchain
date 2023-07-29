mod archive;
mod data;
mod extract;
mod git;
mod github;
mod readme;
mod repository;
mod site;

use std::collections::HashMap;
use crate::repository::index::RepositoryIndex;
use clap::{Parser, Subcommand};
use std::fs::File;
use std::io;
use std::io::{BufReader, BufWriter};

use crate::extract::download_packages;
use crate::git::GitFastImporter;
use crate::repository::package::RepositoryPackage;

use cli_table::{Cell, Style, Table};
use git2::{BranchType, Repository};
use itertools::Itertools;
use rand::thread_rng;

use crate::github::GithubError;
use humansize::DECIMAL;
use indicatif::ParallelProgressIterator;
use rand::seq::SliceRandom;
use rayon::prelude::*;

use rusqlite::Connection;
use std::path::PathBuf;
use std::thread::sleep;
use std::time::Duration;
use serde::Serialize;
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

        input_dir: PathBuf,
    },

    // Creation/bootstrap commands
    UpdateRepos {
        #[clap(short, long)]
        sqlite_file: PathBuf,

        #[clap(short, long, default_value = "40000")]
        chunk_size: usize,

        #[clap(short, long, default_value = "10")]
        limit: usize,

        #[clap(short, long)]
        dry_run: bool,

        // #[clap(long, env)]
        // after: Option<DateTime<Utc>>,
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
    ListRepositories {
        #[clap(long, short)]
        progress_less_than: Option<usize>,

        #[clap(long, short)]
        sample: Option<usize>,

        #[clap(long, env)]
        github_token: String,

        #[clap(long, env)]
        json: bool,
    },
    StaticSite {
        #[clap(long, env)]
        github_token: String,

        #[clap(short, long)]
        content_directory: PathBuf,

        #[clap(short, long)]
        dev: bool,

        #[clap(short, long)]
        reload_from: Option<PathBuf>,
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
            input_dir,
        } => {
            data::merge_parquet_files(&input_dir, &output_file)?;
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
        Commands::ListRepositories {
            github_token,
            progress_less_than,
            sample,
            json
        } => {
            let all_repos = github::projects::get_all_pypi_data_repos(&github_token)?;

            let client = github::get_client();
            let mut repos: Vec<_> = all_repos
                .into_par_iter()
                .progress()
                .flat_map(|repo| {
                    let index =
                        github::index::get_repository_index(&repo.name, Some(client.clone()))?;
                    let stats = index.stats();
                    let projects: _ = index.into_packages().into_iter().counts_by(|p| p.project_name);
                    Ok::<(crate::github::projects::DataRepo, _, _), GithubError>((
                        repo,
                        stats,
                        projects
                    ))
                })
                .collect();

            if let Some(less_than) = progress_less_than {
                repos.retain(|(_, progress, _)| progress.percent_done() < less_than);
            }

            if let Some(sample) = sample {
                let mut rng = thread_rng();
                repos.shuffle(&mut rng);
                repos.drain(sample..);
            }

            if json {
                pub fn sorted_map<S: serde::Serializer, K: Serialize + Ord, V: Serialize>(
                    value: &HashMap<K, V>,
                    serializer: S,
                ) -> Result<S::Ok, S::Error> {
                    value
                        .iter()
                        .sorted_by_key(|v| v.0)
                        .collect::<std::collections::BTreeMap<_, _>>()
                        .serialize(serializer)
                }

                #[derive(Serialize)]
                struct JsonOutput {
                    name: String,
                    stats: crate::repository::index::RepoStats,
                    percent_done: f64,
                    size: usize,
                    #[serde(serialize_with = "sorted_map")]
                    projects: HashMap<String, usize>,
                    url: String,
                }

                let repos: Vec<_> = repos
                    .into_iter()
                    .map(|(repo, stats, projects)| {
                        serde_json::json!({
                            "name": repo.name,
                            "stats": stats,
                            "percent_done": stats.percent_done(),
                            "size": repo.size,
                            "projects": projects,
                            "url": format!("https://github.com/pypi-data/{}", repo.name),
                        })
                    })
                    .collect();
                println!("{}", serde_json::to_string_pretty(&repos)?);
            } else {
                for (repo, _, _) in repos {
                    println!("{}", repo.name);
                }
            }
        }
        Commands::Status { github_token } => {
            let repo_status = github::status::get_status(&github_token, false, None)?;
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
        Commands::StaticSite {
            github_token,
            content_directory,
            dev,
            reload_from,
        } => {
            let limit = if dev { Some(5) } else { None };
            let repo_status = match reload_from {
                None => github::status::get_status(&github_token, true, limit)?,
                Some(p) => {
                    if !p.exists() {
                        let status = github::status::get_status(&github_token, true, limit)?;
                        let mut output = BufWriter::new(File::create(&p)?);
                        serde_json::to_writer_pretty(&mut output, &status)?;
                    }
                    println!("Loading status from {:?}", p);
                    let reader = BufReader::new(File::open(p)?);
                    serde_json::from_reader(reader)?
                }
            };
            if !content_directory.exists() {
                std::fs::create_dir(&content_directory)?;
            }
            let page_limit = if dev { Some(10_000) } else { None };
            println!("Generating site");
            site::static_site::create_repository_pages(
                &content_directory,
                repo_status,
                page_limit,
            )?;
            println!("Generated site");
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

        Commands::UpdateRepos {
            sqlite_file,
            chunk_size,
            limit,
            github_token,
            dry_run,
        } => {
            let client = github::get_client();
            let mut all_repos = github::projects::get_all_pypi_data_repos(&github_token)?;
            all_repos.sort_by_key(|r| r.repo_index_integer());
            all_repos.reverse();
            let last_repo = &all_repos[0];
            let mut repo_index =
                crate::github::index::get_repository_index(&last_repo.name, Some(client.clone()))?;
            let latest_package = repo_index.stats().latest_package;
            let formatted_time = format!("{latest_package:?}");

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
            println!(
                "Repo {} has {}/{}. {:#?}",
                repo_index.index(),
                repo_index.packages().len(),
                repo_index.max_capacity(),
                repo_index.stats()
            );

            if repo_index.has_capacity() {
                let mut extra_capacity = repo_index.extra_capacity();
                let mut collector = vec![];
                while extra_capacity > 0 {
                    if let Some(package) = packages.next() {
                        collector.push(package);
                        extra_capacity -= 1;
                    } else {
                        break;
                    }
                }
                if !collector.is_empty() {
                    let new_package_len = collector.len();
                    repo_index.fill_packages(collector);
                    // repo_index.to_file(&output_dir.join(index.file_name()))?;
                    println!(
                        "Updated last index {} with {} packages. Extra capacity: {}",
                        repo_index.file_name(),
                        new_package_len,
                        repo_index.extra_capacity()
                    );
                    let contents = repo_index.to_string()?;
                    let stats = repo_index.stats();
                    let description = format!(
                        "Code uploaded to PyPI between {} and {}",
                        stats.earliest_package.format("%Y-%m-%d"),
                        stats.latest_package.format("%Y-%m-%d"),
                    );
                    if dry_run {
                        println!(
                            "Would upload index file to repo {}. Description: {}",
                            last_repo.name, description
                        );
                    } else {
                        crate::github::index::upload_index_file(
                            &client,
                            &github_token,
                            &format!("pypi-data/{}", last_repo.name),
                            contents,
                        )?;
                        crate::github::create::update_description(
                            &client,
                            &github_token,
                            &format!("pypi-data/{}", last_repo.name),
                            description,
                        )?;
                    }
                }
                // println!("Index {} set to {}/{} packages", repo_index.index(), repo_index.packages().len(), repo_index.max_capacity());
            }
            let mut max_repo_index = last_repo.repo_index_integer();
            let template_data = github::create::get_template_data(&client, &github_token)?;

            for chunk_iter in packages.chunks(chunk_size).into_iter().take(limit) {
                max_repo_index += 1;
                let chunk = chunk_iter.collect_vec();
                let idx = RepositoryIndex::new(max_repo_index, chunk_size, &chunk);
                let stats = idx.stats();
                let description = format!(
                    "Code uploaded to PyPI between {} and {}",
                    stats.earliest_package.format("%Y-%m-%d"),
                    stats.latest_package.format("%Y-%m-%d"),
                );

                if dry_run {
                    println!(
                        "Would create repo {} with description {}",
                        idx.index(),
                        description
                    );
                    continue;
                }

                let result = github::create::create_repository(
                    &client,
                    &github_token,
                    &template_data,
                    idx.index(),
                    description,
                )?;
                println!("Created repository for index: {}. Sleeping", idx.index());
                sleep(Duration::from_secs(4));
                github::create::create_deploy_key(&client, &github_token, &result)?;
                github::index::upload_index_file(
                    &client,
                    &github_token,
                    &result,
                    idx.to_string()?,
                )?;
                println!(
                    "Finished creating repository for index: {}. Sleeping",
                    idx.index()
                );
                sleep(Duration::from_secs(4));
            }
        }
        // Commands::UpdateRepositories {
        //     output_dir,
        //     index_paths,
        //     github_token,
        // } => {
        //     std::fs::create_dir_all(&output_dir)?;
        //     let client = github::get_client();
        //     let template_data = github::create::get_template_data(&client, &github_token)?;
        //
        //     for index_path in index_paths {
        //         println!("Creating repository for index: {}", index_path.display());
        //         let idx = RepositoryIndex::from_path(&index_path)?;
        //         let stats = idx.stats();
        //
        //         let result = github::create::create_repository(
        //             &client,
        //             &github_token,
        //             &template_data,
        //             idx.index(),
        //             format!(
        //                 "Code uploaded to PyPi between {} and {}",
        //                 stats.earliest_package.format("%Y-%m-%d"),
        //                 stats.latest_package.format("%Y-%m-%d"),
        //             ),
        //         )?;
        //         println!(
        //             "Created repository for index: {}. Sleeping",
        //             index_path.display()
        //         );
        //         sleep(Duration::from_secs(4));
        //         github::create::create_deploy_key(&client, &github_token, &result)?;
        //         github::index::upload_index_file(&client, &github_token, &result, &index_path)?;
        //         std::fs::copy(
        //             &index_path,
        //             output_dir.join(index_path.file_name().unwrap()),
        //         )?;
        //         println!(
        //             "Finished creating repository for index: {}. Sleeping",
        //             index_path.display()
        //         );
        //         sleep(Duration::from_secs(4));
        //     }
        // }
        Commands::GetAllIndexes {
            output_dir,
            github_token,
        } => {
            std::fs::create_dir_all(&output_dir)?;
            let all_repos = github::projects::get_all_pypi_data_repos(&github_token)?;
            // let client = github::get_client();
            let res: Vec<Result<_, _>> = all_repos
                .into_par_iter()
                .progress()
                .map(|repo| {
                    let output_path = output_dir.join(format!("{}.parquet", repo.name));
                    let etag_path = output_dir.join(format!("{}.etag", repo.name));
                    // let mut output_file =
                    //     std::io::BufWriter::new(std::fs::File::create(&output_path).unwrap());
                    let url = format!(
                        "https://github.com/pypi-data/{}/releases/download/latest/dataset.parquet",
                        repo.name
                    );
                    let result = duct::cmd!(
                        "curl",
                        &url,
                        "-o",
                        &output_path,
                        "--silent",
                        "--fail",
                        "--retry",
                        "5",
                        "--retry-delay",
                        "3",
                        "--location",
                        "-w",
                        "%{http_code}",
                        "--remote-time",
                        "--remove-on-error",
                        "--etag-compare",
                        &etag_path,
                        "--etag-save",
                        &etag_path
                    )
                        .unchecked()
                        .stdout_capture()
                        .stderr_null()
                        .run()?;

                    let stdout = std::str::from_utf8(&result.stdout)?;

                    match (result.status.success(), stdout) {
                        (true, _) => Ok(()),
                        (false, "404") => Ok(()),
                        (false, status) => Err(anyhow::anyhow!(
                            "Failed to download {url} with status {status}"
                        )),
                    }
                })
                .collect();
            for item in res {
                item?;
            }
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
    }
    Ok(())
}
