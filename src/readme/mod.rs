use crate::repository::index::RepositoryIndex;

use anyhow::{Result};

use itertools::Itertools;
use prettytable::format::consts::FORMAT_NO_LINESEP_WITH_TITLE;
use prettytable::format::{TableFormat};
use prettytable::{row, Table};
use serde::Serialize;

use tinytemplate::TinyTemplate;

#[derive(Serialize)]
struct Context {
    name: String,
    total_packages: usize,
    first_package_time: String,
    last_package_time: String,
    package_table: String,
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

    let format: TableFormat = *FORMAT_NO_LINESEP_WITH_TITLE; //FormatBuilder::new().column_separator()
    let mut table = Table::new();
    table.set_format(format);

    table.set_titles(row!["count"]);

    for (name, count) in packages_by_count
        .into_iter()
        .sorted_by(|v1, v2| v1.1.cmp(&v2.1).reverse())
    {
        table.add_row(row![name, count]);
    }

    let mut writer = vec![];
    table.print_html(&mut writer)?;
    let table = String::from_utf8(writer)?;

    let context = Context {
        name: "World".to_string(),
        total_packages: index.packages().len(),
        first_package_time: format!("{}", index.first_package_time().format("%Y-%m-%d %H:%M")),
        last_package_time: format!("{}", index.last_package_time().format("%Y-%m-%d %H:%M")),
        package_table: table,
    };

    let rendered = tt.render("readme", &context)?;
    return Ok(rendered);
}
