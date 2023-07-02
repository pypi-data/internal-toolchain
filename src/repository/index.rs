use crate::repository::package::RepositoryPackage;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io;
use std::io::{BufReader, BufWriter, ErrorKind};

use chrono::{DateTime, Utc};
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
}

impl RepositoryIndex {
    pub fn new(index: usize, max_capacity: usize, packages: &[RepositoryPackage]) -> Self {
        RepositoryIndex {
            index,
            max_capacity,
            packages: packages.to_vec(),
        }
    }

    pub fn from_path(path: &Path) -> Result<Self, RepositoryIndexError> {
        let file = File::open(&path).map_err(|e| match e.kind() {
            ErrorKind::NotFound => RepositoryIndexError::NotFound(path.to_path_buf()),
            _ => RepositoryIndexError::IOError(e),
        })?;
        let reader = BufReader::new(file);
        let content: RepositoryIndex = serde_json::from_reader(reader)?;
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

    pub fn last_package_time(&self) -> DateTime<Utc> {
        let max_item = self
            .packages
            .iter()
            .max_by(|v1, v2| v1.upload_time.cmp(&v2.upload_time))
            .unwrap();
        max_item.upload_time
    }

    pub fn first_package_time(&self) -> DateTime<Utc> {
        let min_item = self
            .packages
            .iter()
            .min_by(|v1, v2| v1.upload_time.cmp(&v2.upload_time))
            .unwrap();
        min_item.upload_time
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

    fn extra_capacity(&self) -> usize {
        self.max_capacity - self.packages.len()
    }

    pub fn fill_packages(&mut self, new_packages: &mut Vec<RepositoryPackage>) {
        let total_new_packages = new_packages.len();
        let capacity = self.extra_capacity();
        let start_index = if capacity > total_new_packages {
            0
        } else {
            total_new_packages - capacity
        };
        let drained = new_packages.drain(start_index..);
        self.packages.extend(drained);
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
