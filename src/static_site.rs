use crate::repository::index::RepositoryIndex;
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
    index: usize,
    package_filename: String,
    package: RepositoryPackage,
}

#[derive(Serialize)]
pub struct PackageContext {
    name: String,
    packages_with_indexes: Vec<PackageWithIndex>,
}

pub fn create_repository_pages(
    template_dir: &Path,
    packages_directory: &Path,
    repo_indexes: Vec<RepositoryIndex>,
) -> Result<(), anyhow::Error> {
    let tera = Tera::new(template_dir.join("*").to_str().unwrap())?;

    let processed_packages = repo_indexes.into_iter().flat_map(|i| {
        let idx = i.index();
        i.into_packages().into_iter().map(move |p| (idx, p))
    });

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

    let total_count = packages_by_name.len();

    packages_by_name
        .into_par_iter()
        .progress_count(total_count as u64)
        .try_for_each(|(name, mut packages_with_indexes)| {
            packages_with_indexes.sort_by_key(|p| p.package.upload_time);
            packages_with_indexes.reverse();

            let content_path = packages_directory.join(format!("{}.html", name));
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