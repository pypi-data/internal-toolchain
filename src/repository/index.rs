use crate::repository::package::RepositoryPackage;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io;
use std::io::{BufReader, BufWriter, ErrorKind};

use chrono::{DateTime, Utc};
use flate2::bufread::GzDecoder;
use itertools::{Itertools, MinMaxResult};

use std::path::{Path, PathBuf};

use thiserror::Error;

#[derive(Debug, Serialize, Deserialize)]
pub struct RepositoryIndex {
    index: usize,
    max_capacity: usize,
    packages: Vec<RepositoryPackage>,
}

impl Display for RepositoryIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RepoPackages index={} capacity={} used={} packages=[",
            self.index,
            self.max_capacity,
            self.packages.len()
        )?;
        for item in &self.packages {
            write!(f, "{}, ", item)?
        }
        write!(f, "]")
    }
}

#[derive(Error, Debug)]
pub enum RepositoryIndexError {
    #[error("Error marshalling repository state: {0}")]
    SerdeError(#[from] serde_json::Error),

    #[error("Error opening repository state: {0}")]
    IOError(#[from] io::Error),

    #[error("Index file not found at {0}")]
    NotFound(PathBuf),

    #[error("Unknown file extension for {0}")]
    UnknownExt(PathBuf),
}

#[derive(Debug, Serialize)]
pub struct RepoStats {
    pub earliest_package: DateTime<Utc>,
    pub latest_package: DateTime<Utc>,
    pub total_packages: usize,
    pub done_packages: usize,
}

impl RepoStats {
    pub fn percent_done(&self) -> usize {
        ((self.done_packages as f64 / self.total_packages as f64) * 100.0) as usize
    }
}

impl RepositoryIndex {
    pub fn new(index: usize, max_capacity: usize, packages: &[RepositoryPackage]) -> Self {
        RepositoryIndex {
            index,
            max_capacity,
            packages: packages.to_vec(),
        }
    }

    pub fn file_name(&self) -> String {
        format!("{}.json", self.index)
    }

    pub fn from_path(path: &Path) -> Result<Self, RepositoryIndexError> {
        let file = File::open(path).map_err(|e| match e.kind() {
            ErrorKind::NotFound => RepositoryIndexError::NotFound(path.to_path_buf()),
            _ => RepositoryIndexError::IOError(e),
        })?;
        let buf_reader = BufReader::new(file);
        let content: Self = match path.extension() {
            Some(ext) if ext == "json" => serde_json::from_reader(buf_reader)?,
            Some(ext) if ext == "gz" => serde_json::from_reader(GzDecoder::new(buf_reader))?,
            _ => return Err(RepositoryIndexError::UnknownExt(path.to_path_buf())),
        };
        Ok(content)
    }

    pub fn to_file(&self, path: &Path) -> Result<(), RepositoryIndexError> {
        let file = File::create(path)?;
        let writer = BufWriter::new(file);
        serde_json::to_writer_pretty(writer, &self)?;
        Ok(())
    }

    pub fn packages(&self) -> &Vec<RepositoryPackage> {
        &self.packages
    }

    pub fn stats(&self) -> RepoStats {
        let minmax_time = self.packages.iter().map(|e| e.upload_time).minmax();

        let (earliest_package, latest_package) = match minmax_time {
            MinMaxResult::OneElement(e) => (e, e),
            MinMaxResult::MinMax(e, l) => (e, l),
            MinMaxResult::NoElements => unreachable!("No packages found"),
        };

        let total_packages = self.packages.len();
        let done_packages = self.packages.iter().filter(|p| p.processed).count();
        RepoStats {
            earliest_package,
            latest_package,
            total_packages,
            done_packages,
        }
    }

    pub fn mark_packages_as_processed(&mut self, packages: Vec<RepositoryPackage>) {
        for package in self.packages.iter_mut() {
            if packages.contains(package) {
                package.set_processed(true)
            }
        }
    }

    pub fn unprocessed_packages(&mut self) -> Vec<RepositoryPackage> {
        self.packages
            .iter()
            .filter(|p| !p.processed)
            .cloned()
            .collect()
    }

    pub fn index(&self) -> usize {
        self.index
    }

    pub fn has_capacity(&self) -> bool {
        self.extra_capacity() > 0
    }

    pub fn extra_capacity(&self) -> usize {
        self.max_capacity - self.packages.len()
    }

    pub fn fill_packages(&mut self, new_packages: Vec<RepositoryPackage>) {
        let total_new_packages = new_packages.len();
        let capacity = self.extra_capacity();
        if capacity < total_new_packages {
            panic!("Index {} panicked while filling packages. Not enough capacity: {capacity} < {total_new_packages}", self.index)
        }
        self.packages.extend(new_packages);
    }
}

impl PartialEq<Self> for RepositoryIndex {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index
    }
}

impl Eq for RepositoryIndex {}

impl Ord for RepositoryIndex {
    fn cmp(&self, other: &Self) -> Ordering {
        self.index.cmp(&other.index)
    }
}

impl PartialOrd for RepositoryIndex {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.index.partial_cmp(&other.index)
    }
}
