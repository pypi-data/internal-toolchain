// use crate::repository::index::RepositoryIndex;
// use crate::repository::package::RepositoryPackage;
// use itertools::Itertools;
// use serde::{Deserialize, Serialize};
// use std::collections::hash_map::RandomState;
// use std::collections::HashSet;
// use std::fs::File;
// use std::io;
// use std::io::{BufReader, BufWriter, ErrorKind};
// use std::ops::Sub;
// use std::path::Path;
// use thiserror::Error;
//
// #[derive(Error, Debug)]
// pub enum RepoStateError {
//     #[error("Error marshalling repository state: {0}")]
//     SerdeError(#[from] serde_json::Error),
//
//     #[error("Error opening repository state: {0}")]
//     IOError(#[from] io::Error),
//
//     #[error("State file not found")]
//     NotFound,
// }
//
// #[derive(Deserialize, Serialize)]
// pub struct RepoState {
//     pending_packages: Vec<String>,
// }
//
// impl RepoState {
//     pub fn new(existing_state: &RepositoryIndex) -> Self {
//         RepoState {
//             pending_packages: existing_state.package_identifiers(),
//         }
//     }
//
//     pub fn read_or_create(
//         path: &Path,
//         existing_state: &RepositoryIndex,
//     ) -> Result<Self, RepoStateError> {
//         match Self::from_path(path) {
//             Ok(r) => Ok(r),
//             Err(RepoStateError::NotFound) => Ok(Self::new(existing_state)),
//             Err(e) => Err(e),
//         }
//     }
//
//     pub fn from_path(path: &Path) -> Result<Self, RepoStateError> {
//         let file = File::open(path).map_err(|e| match e.kind() {
//             ErrorKind::NotFound => RepoStateError::NotFound,
//             _ => RepoStateError::IOError(e),
//         })?;
//         let reader = BufReader::new(file);
//         let content: RepoState = serde_json::from_reader(reader)?;
//         Ok(content)
//     }
//
//     pub fn to_file(&self, path: &Path) -> Result<(), RepoStateError> {
//         let file = File::create(path)?;
//         let writer = BufWriter::new(file);
//         serde_json::to_writer_pretty(writer, &self)?;
//         Ok(())
//     }
//
//     pub fn unprocessed_packages<'a>(
//         &self,
//         packages: &'a Vec<RepositoryPackage>,
//     ) -> Vec<&'a RepositoryPackage> {
//         packages
//             .iter()
//             .filter(|p| self.pending_packages.contains(&p.identifier()))
//             .collect()
//     }
//
//     pub fn add_processed_packages(&mut self, packages: Vec<&RepositoryPackage>) {
//         let pending_package_set: HashSet<_, RandomState> =
//             HashSet::from_iter(self.pending_packages.drain(0..));
//         let processed_package_set = HashSet::from_iter(packages.iter().map(|v| v.identifier()));
//         let new_pending_packages = pending_package_set.sub(&processed_package_set);
//         self.pending_packages = new_pending_packages.into_iter().collect();
//         self.pending_packages.sort();
//     }
// }
