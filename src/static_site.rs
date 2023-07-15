use crate::repository::index::RepositoryIndex;
use chrono::{DateTime, Utc};
use indicatif::ParallelProgressIterator;
use itertools::Itertools;
use rayon::prelude::*;
use serde::Serialize;
use std::path::Path;
use url::Url;

#[derive(Serialize)]
pub struct FrontMatter {
    pub title: String,
    pub in_search_index: bool,
    pub template: &'static str,
    pub extra: Extra,
    pub path: String
}

#[derive(Serialize)]
pub struct TransparentFrontMatter {
    pub transparent: bool,
    pub render: bool,
}

#[derive(Serialize)]
pub struct Extra {
    package_count: usize,
    versions: Vec<Version>,
}

#[derive(Serialize)]
pub struct Version {
    version: String,
    url: Url,
    filename: String,
    uploaded: DateTime<Utc>,
    index: usize,
    processed: bool,
    pub denormalized_name: String,
}

pub fn create_repository_pages(
    packages_directory: &Path,
    repo_indexes: Vec<RepositoryIndex>,
) -> Result<(), anyhow::Error> {
    let transparent_content = toml::to_string_pretty(&TransparentFrontMatter {
        transparent: false,
        render: false,
    })?;
    std::fs::write(
        packages_directory.join("_index.md"),
        format!("+++\n{transparent_content}\n+++"),
    )?;

    let processed_packages = repo_indexes
        .into_iter()
        .flat_map(|i| {
            let idx = i.index();
            i.into_packages().into_iter().map(move |p| (idx, p))
        });
    let packages_by_name = processed_packages
        .map(|(idx, p)| {
            let mut name = p.project_name.replace(['_', '.'], "-");
            name.make_ascii_lowercase();
            (name, (idx, p))
        })
        .into_group_map();
    let total_count = packages_by_name.len();

    packages_by_name
        .into_par_iter()
        .progress_count(total_count as u64)
        .try_for_each(|(name, mut packages)| {
            packages.sort_by_key(|p| p.1.upload_time);
            packages.reverse();
            let versions: Vec<_> = packages
                .into_iter()
                .map(|(idx, p)| {
                    let filename = p.package_filename().to_string();
                    Version {
                        denormalized_name: p.project_name,
                        version: p.project_version,
                        url: p.url,
                        filename,
                        uploaded: p.upload_time,
                        index: idx,
                        processed: p.processed
                    }
                })
                .collect();

            let mut indices = name.char_indices().skip(1);
            let prefix = match indices.next() {
                None => &name[..],
                Some(_) => match indices.next() {
                    None => &name[..],
                    Some((end_idx, _)) => &name[0..end_idx],
                },
            };
            let content_dir = packages_directory.join(prefix);
            let content_path = content_dir.join(format!("{}.md", name));
            let path = format!("packages/{name}");
            std::fs::create_dir_all(&content_dir)?;

            // let index_path = content_dir.join("_index.md");
            // if !index_path.exists() {
            //     std::fs::write(&index_path, format!("+++\n{transparent_content}\n+++"))?;
            // }
            let front_matter = FrontMatter {
                title: name,
                in_search_index: true,
                template: "package.html",
                path,
                extra: Extra {
                    package_count: versions.len(),
                    versions,
                },
            };
            let content = toml::to_string_pretty(&front_matter)?;
            std::fs::write(content_path, format!("+++\n{content}\n+++"))?;
            Ok::<_, anyhow::Error>(())
        })?;
    Ok(())
}
