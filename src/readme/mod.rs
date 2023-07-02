use crate::repository::index::RepositoryIndex;

use anyhow::Result;

use itertools::Itertools;



use serde::Serialize;

use tinytemplate::TinyTemplate;

#[derive(Serialize)]
struct Context {
    name: String,
    total_packages: usize,
    first_package_time: String,
    last_package_time: String,
    table_data: Vec<(String, i32)>,
    done_count: usize,
    percent_done: usize,
}

pub fn generate_readme(index: RepositoryIndex) -> Result<String> {
    let mut tt = TinyTemplate::new();
    tt.add_template("readme", include_str!("readme_template.md"))?;
    tt.set_default_formatter(&tinytemplate::format_unescaped);

    let packages_by_count = index
        .packages()
        .clone()
        .into_iter()
        .into_grouping_map_by(|a| a.project_name.clone())
        .fold(0, |acc, _key, _val| acc + 1);

    let table_data: Vec<_> = packages_by_count
        .into_iter()
        .sorted_by(|v1, v2| v1.1.cmp(&v2.1).reverse())
        .collect();

    let total_packages = index.packages().len();

    let done_count = index.packages().iter().filter(|p| p.processed).count();
    let percent_done = ((done_count as f64 / total_packages as f64) * 100.0) as usize;

    let context = Context {
        name: "World".to_string(),
        total_packages,
        first_package_time: format!("{}", index.first_package_time().format("%Y-%m-%d %H:%M")),
        last_package_time: format!("{}", index.last_package_time().format("%Y-%m-%d %H:%M")),
        table_data,
        done_count,
        percent_done,
    };

    let rendered = tt.render("readme", &context)?;
    return Ok(rendered);
}
