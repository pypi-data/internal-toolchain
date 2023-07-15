use std::collections::HashMap;
use crate::github::status::RepoStatus;

use crate::repository::package::RepositoryPackage;

use indicatif::ParallelProgressIterator;
use itertools::Itertools;
use minify_html::{minify, Cfg};
use rayon::prelude::*;
use serde::Serialize;
use std::path::Path;
use tera::{Context, Tera};

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
    template_dir: &Path,
    root_dir: &Path,
    repo_status: Vec<RepoStatus>,
    limit: Option<usize>,
) -> Result<(), anyhow::Error> {
    let tera = Tera::new(template_dir.join("*").to_str().unwrap())?;

    // Status page
    let index_content = tera.render(
        "status.html",
        &Context::from_serialize(IndexContext { data: &repo_status })?,
    )?;
    std::fs::write(root_dir.join("index.html"), index_content)?;

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

    let index_content = tera.render(
        "package_search.html",
        &Context::from_serialize(PackageSearchContext {
            total_packages: packages_by_name.len(),
            packages: packages_by_name.keys().collect_vec()
        })?,
    )?;
    std::fs::write(packages_directory.join("index.html"), index_content)?;

    let total_count = packages_by_name.len();

    packages_by_name
        .into_par_iter()
        .progress_count(total_count as u64)
        .try_for_each(|(name, mut packages_with_indexes)| {
            packages_with_indexes.sort_by_key(|p| p.package.upload_time);
            packages_with_indexes.reverse();
            let content_dir = packages_directory.join(&name);
            std::fs::create_dir_all(&content_dir)?;
            let content_path = content_dir.join("index.html");
            let ctx = PackageContext {
                name,
                packages_with_indexes,
            };

            let content = tera.render("package.html", &Context::from_serialize(ctx)?)?;
            let minify_cfg = Cfg::spec_compliant();
            let minified = minify(content.as_bytes(), &minify_cfg);
            std::fs::write(content_path, minified)?;
            Ok::<_, anyhow::Error>(())
        })?;

    Ok(())
}
