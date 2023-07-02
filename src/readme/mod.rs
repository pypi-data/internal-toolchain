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
        .sorted_by(|v1, v2| v1.1.cmp(&v2.1).then(v1.0.cmp(&v2.0)).reverse())
        .take(25)
        .collect();

    let stats = index.stats();

    let context = Context {
        name: format!("PyPi code {}", index.index()),
        total_packages: stats.total_packages,
        first_package_time: format!("{}", stats.earliest_package.format("%Y-%m-%d %H:%M")),
        last_package_time: format!("{}", stats.latest_package.format("%Y-%m-%d %H:%M")),
        table_data,
        done_count: stats.done_packages,
        percent_done: stats.percent_done(),
    };

    let rendered = tt.render("readme", &context)?;
    return Ok(rendered);
}
