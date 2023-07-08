use crate::archive::tar::{iter_tar_bz_contents, iter_tar_gz_contents};
use crate::archive::{ArchiveItem, ArchiveType, ExtractionError};
use crate::data::{IndexItem, PackageFileIndex, RepositoryFileIndexWriter};
use crate::git::GitFastImporter;

use crate::repository::package::RepositoryPackage;
use anyhow::Result;
use bzip2::read::BzDecoder;
use flate2::read::GzDecoder;
use indicatif::ParallelProgressIterator;
use rayon::prelude::*;
use std::ffi::OsStr;
use std::io::{BufReader, BufWriter, Stdout, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;
use std::{io, panic};
use tar::Archive;
use thiserror::Error;
use tracing::{event, span, Level};
use ureq::{Agent, Error, Transport};

#[derive(Error, Debug)]
pub enum DownloadError {
    #[error("Package is missing from the index")]
    Missing,

    #[error("Unexpected status: {0}")]
    UnexpectedStatus(u16),

    #[error("Transport error: {0}")]
    TransportError(#[from] Box<Transport>),

    #[error("There was an error writing the package data: {0}")]
    WriteError(#[from] io::Error),

    #[error("Unknown archive type: {0}")]
    UnknownArchive(String),

    #[error("Extraction Error: {0}")]
    ExtractionError(#[from] ExtractionError),

    #[error("Panic Error: {0}")]
    PanicError(String),
}

pub fn download_packages(
    packages: Vec<RepositoryPackage>,
    index_file: PathBuf,
    output: Mutex<GitFastImporter<BufWriter<Stdout>>>,
) -> Result<Vec<RepositoryPackage>, DownloadError> {
    let total = packages.len() as u64;

    let _span = span!(Level::INFO, "started_downloading_packages", total = total).entered();
    let index_writer = RepositoryFileIndexWriter::new(&index_file);

    event!(Level::INFO, "starting par_iter");
    let processed_packages: Vec<_> = packages
        .into_par_iter()
        .progress_count(total)
        .flat_map(|package| {
            let _span = span!(
                Level::INFO,
                "downloading_package",
                id = package.identifier()
            )
            .entered();
            event!(Level::INFO, "download started");
            let panic: Result<_, _> = panic::catch_unwind(|| {
                let agent = ureq::agent();
                Ok(download_package(agent, &package, &output)?)
            });
            event!(Level::INFO, "extraction finished");
            let result = match panic {
                Ok(r) => r,
                Err(err) => {
                    event!(Level::ERROR, "download_package panicked");
                    if let Some(s) = err.downcast_ref::<String>() {
                        return Err(DownloadError::PanicError(s.clone()));
                    } else if let Some(s) = err.downcast_ref::<&str>() {
                        return Err(DownloadError::PanicError(s.to_string()));
                    } else {
                        event!(
                            Level::ERROR,
                            "Unknown download_package panic, resuming unwind"
                        );
                        eprintln!("Unknown panic type: {:?}", err.type_id());
                        panic::resume_unwind(err);
                    }
                }
            };
            let index_items = match result {
                Ok(idx) => idx,
                Err(e) => {
                    return match e {
                        DownloadError::Missing => Ok(package),
                        _ => Err(e),
                    };
                }
            };
            event!(Level::DEBUG, "writing index");
            index_writer.lock().unwrap().write_index(index_items);
            event!(Level::DEBUG, "index written");
            Ok(package)
        })
        .collect();

    event!(Level::INFO, "Finishing output");
    output.lock().unwrap().finish()?;
    Ok(processed_packages)
}

fn write_package_contents<
    T: Iterator<Item = Result<(IndexItem, Option<ArchiveItem>), ExtractionError>>,
    O: Write,
>(
    package: &RepositoryPackage,
    mut contents: T,
    output: &Mutex<GitFastImporter<O>>,
) -> Result<Vec<IndexItem>, ExtractionError> {
    let _span = span!(
        Level::INFO,
        "writing_package_contents",
        id = package.identifier()
    )
    .entered();
    let mut path_to_nodes = vec![];
    let mut index_items = vec![];
    let mut error = None;

    for result in contents.by_ref() {
        let (index_item, item) = match result {
            Ok(v) => v,
            Err(e) => {
                error = Some(e);
                break;
            }
        };
        if let Some(item) = item {
            let node = match output.lock().unwrap().add_file(item.data) {
                Ok(v) => v,
                Err(e) => {
                    error = Some(e.into());
                    break;
                }
            };
            path_to_nodes.push((node, item.path));
        }
        index_items.push(index_item);
    }

    if let Some(e) = error {
        event!(
            Level::ERROR,
            "Error writing package contents: {:?}. Consuming iterator",
            e
        );
        // let left_items = contents.count();
        // event!(Level::ERROR, "Skipped {} items due to error", left_items);
        return Err(e);
    }

    event!(Level::INFO, "Flushing commit");
    output
        .lock()
        .unwrap()
        .flush_commit(&package.identifier(), path_to_nodes)?;
    event!(Level::INFO, "Commit flushed");
    Ok(index_items)
}

pub fn download_package<'a, O: Write>(
    agent: Agent,
    package: &'a RepositoryPackage,
    output: &Mutex<GitFastImporter<O>>,
) -> Result<PackageFileIndex<'a>, DownloadError> {
    let resp = agent
        .request_url("GET", &package.url)
        .timeout(Duration::from_secs(1))
        .call()
        .map_err(|e| match e {
            Error::Status(404, _) => DownloadError::Missing,
            Error::Status(status, _) => DownloadError::UnexpectedStatus(status),
            Error::Transport(t) => DownloadError::TransportError(t.into()),
        })?;

    let mut reader = BufReader::new(resp.into_reader());
    let path = Path::new(package.url.path());
    let extension = path.extension().and_then(OsStr::to_str).unwrap();
    let archive_type: ArchiveType = extension
        .parse()
        .map_err(|_| DownloadError::UnknownArchive(extension.to_string()))?;

    let items = match archive_type {
        ArchiveType::Zip => {
            let iterator = std::iter::from_fn(|| {
                crate::archive::zip::iter_zip_package_contents(&mut reader, package.file_prefix())
            });
            write_package_contents(package, iterator, output)?
        }
        ArchiveType::TarGz => {
            let tar = GzDecoder::new(reader);
            let mut archive = Archive::new(tar);
            let iterator = iter_tar_gz_contents(&mut archive, package.file_prefix())?;
            write_package_contents(package, iterator, output)?
        }
        ArchiveType::TarBz => {
            let tar = BzDecoder::new(reader);
            let mut archive = Archive::new(tar);
            let iterator = iter_tar_bz_contents(&mut archive, package.file_prefix())?;
            write_package_contents(package, iterator, output)?
        }
    };
    let package_index = PackageFileIndex::new(package, items);
    Ok(package_index)
}
