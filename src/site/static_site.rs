use crate::github::status::RepoStatus;

use crate::repository::package::RepositoryPackage;

use indicatif::ParallelProgressIterator;
use itertools::Itertools;
use rayon::prelude::*;
use serde::Serialize;
use std::path::Path;
use flate2::Compression;
use flate2::write::ZlibEncoder;

#[derive(Serialize)]
pub struct PackageWithIndex {
    pub index: usize,
    pub package_filename: String,
    pub package: RepositoryPackage,
}

#[derive(Serialize)]
pub struct PackageContext {
    name: String,
    packages_with_indexes: Vec<PackageWithIndex>,
}

#[derive(Serialize)]
pub struct IndexContext<'a> {
    data: &'a Vec<RepoStatus>,
}

#[derive(Serialize)]
pub struct PackageSearchContext<'a> {
    total_packages: usize,
    packages: Vec<&'a String>,
}

pub fn create_repository_pages(
    root_dir: &Path,
    repo_status: Vec<RepoStatus>,
    limit: Option<usize>,
) -> Result<(), anyhow::Error> {
    // Packages:
    let packages_directory = root_dir.join("packages/");
    if !packages_directory.exists() {
        std::fs::create_dir(&packages_directory)?;
    }
    let repo_indexes = repo_status.into_iter().map(|s| s.index);
    let processed_packages = repo_indexes
        .into_iter()
        .flat_map(|i| {
            let idx = i.index();
            i.into_packages().into_iter().map(move |p| (idx, p))
        })
        .take(limit.unwrap_or(usize::MAX));

    let packages_by_name = processed_packages
        .map(|(idx, p)| {
            let mut name = p.project_name.replace(['_', '.'], "-");
            name.make_ascii_lowercase();
            (
                name,
                PackageWithIndex {
                    index: idx,
                    package_filename: p.package_filename().to_string(),
                    package: p,
                },
            )
        })
        .into_group_map();

    let package_list = PackageSearchContext {
        total_packages: packages_by_name.len(),
        packages: packages_by_name.keys().sorted().collect_vec(),
    };

    let index_writer = std::io::BufWriter::new(std::fs::File::create(root_dir.join("pages.json"))?);
    let index_writer = ZlibEncoder::new(index_writer, Compression::best());
    serde_json::to_writer(index_writer, &package_list)?;

    let total_count = packages_by_name.len();

    packages_by_name
        .into_par_iter()
        .progress_count(total_count as u64)
        .try_for_each(|(name, mut packages_with_indexes)| {
            packages_with_indexes.sort_by_key(|p| p.package.upload_time);
            packages_with_indexes.reverse();
            let first_char_of_name = name.chars().next().unwrap();
            let content_dir = packages_directory.join(first_char_of_name.to_string());
            std::fs::create_dir_all(&content_dir)?;
            let content_path = content_dir.join(format!("{name}.json"));
            let writer = std::io::BufWriter::new(std::fs::File::create(content_path)?);
            let writer = ZlibEncoder::new(writer, Compression::best());
            serde_json::to_writer(
                writer,
                &PackageContext {
                    name,
                    packages_with_indexes,
                },
            )?;
            Ok::<_, anyhow::Error>(())
        })?;

    Ok(())
}
