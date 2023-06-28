use crate::archive::tar::{iter_tar_bz_contents, iter_tar_gz_contents};
use crate::archive::{ArchiveItem, ArchiveType};
use crate::data::{IndexItem, PackageFileIndex, RepositoryFileIndexWriter};
use crate::git::GitFastImporter;

use crate::repository::package::RepositoryPackage;
use anyhow::Result;
use bzip2::read::BzDecoder;
use flate2::read::GzDecoder;
use indicatif::ParallelProgressIterator;
use rayon::prelude::*;
use std::ffi::OsStr;
use std::io;
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use tar::Archive;
use thiserror::Error;
use ureq::{Agent, Error, Transport};

#[derive(Error, Debug)]
pub enum DownloadError {
    #[error("Package is missing from the index")]
    Missing,

    #[error("Unexpected status: {0}")]
    UnexpectedStatus(u16),

    #[error("Transport error: {0}")]
    TransportError(#[from] Transport),

    #[error("There was an error writing the package data: {0}")]
    WriteError(#[from] io::Error),

    #[error("Unknown archive type: {0}")]
    UnknownArchive(String),
}

pub fn download_packages(
    packages: Vec<RepositoryPackage>,
    index_file: PathBuf,
) -> Result<Vec<RepositoryPackage>, DownloadError> {
    let agent = ureq::agent();

    let output = GitFastImporter::new(std::io::BufWriter::new(io::stdout()), "foo".to_string());
    let total = packages.len() as u64;

    let index_writer = RepositoryFileIndexWriter::new(&index_file);

    let processed_packages: Vec<_> = packages
        .into_par_iter()
        .progress_count(total)
        .flat_map(|package| {
            let index_items = match download_package(agent.clone(), &package, &output) {
                Ok(idx) => idx,
                Err(e) => {
                    return match e {
                        DownloadError::Missing => Ok(package),
                        _ => Err(e),
                    };
                }
            };
            index_writer.lock().unwrap().write_index(index_items);
            Ok(package)
        })
        .collect();

    output.lock().unwrap().finish()?;
    Ok(processed_packages)
}

fn write_package_contents<T: Iterator<Item = (IndexItem, Option<ArchiveItem>)>, O: Write>(
    package: &RepositoryPackage,
    contents: T,
    output: &Mutex<GitFastImporter<O>>,
) -> io::Result<Vec<IndexItem>> {
    let mut path_to_nodes = vec![];
    let mut index_items = vec![];
    for (index_item, item) in contents {
        if let Some(item) = item {
            let node = output.lock().unwrap().add_file(item.data)?;
            path_to_nodes.push((node, item.path))
        }
        index_items.push(index_item);
    }
    output
        .lock()
        .unwrap()
        .flush_commit(path_to_nodes, Some(package.file_prefix()))?;
    Ok(index_items)
}

fn download_package<'a, O: Write>(
    agent: Agent,
    package: &'a RepositoryPackage,
    output: &Mutex<GitFastImporter<O>>,
) -> Result<PackageFileIndex<'a>, DownloadError> {
    let resp = agent
        .request_url("GET", &package.url)
        .call()
        .map_err(|e| match e {
            Error::Status(404, _) => DownloadError::Missing,
            Error::Status(status, _) => DownloadError::UnexpectedStatus(status),
            Error::Transport(t) => DownloadError::TransportError(t),
        })?;

    let mut reader = BufReader::new(resp.into_reader());
    let path = Path::new(package.url.path());
    let extension = path.extension().and_then(OsStr::to_str).unwrap();
    let archive_type: ArchiveType = extension
        .parse()
        .map_err(|_| DownloadError::UnknownArchive(extension.to_string()))?;

    let items = match archive_type {
        ArchiveType::Zip => {
            let iterator =
                std::iter::from_fn(|| crate::archive::zip::iter_zip_package_contents(&mut reader));
            write_package_contents(package, iterator, output)?
        }
        ArchiveType::TarGz => {
            let tar = GzDecoder::new(reader);
            let mut archive = Archive::new(tar);
            write_package_contents(package, iter_tar_gz_contents(&mut archive), output)?
        }
        ArchiveType::TarBz => {
            let tar = BzDecoder::new(reader);
            let mut archive = Archive::new(tar);
            write_package_contents(package, iter_tar_bz_contents(&mut archive), output)?
        }
    };
    let package_index = PackageFileIndex::new(package, items);
    Ok(package_index)
}
