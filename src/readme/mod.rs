use crate::repository::index::RepositoryIndex;

use anyhow::Result;

use serde::Serialize;

use tinytemplate::TinyTemplate;

#[derive(Serialize)]
struct Context {
    name: String,
    total_packages: usize,
    first_package_time: String,
    last_package_time: String,
    done_count: usize,
    percent_done: usize,
    view_url: String,
    code_url: String
}

pub fn generate_readme(index: RepositoryIndex) -> Result<String> {
    let mut tt = TinyTemplate::new();
    tt.add_template("readme", include_str!("readme_template.md"))?;
    tt.set_default_formatter(&tinytemplate::format_unescaped);

    let stats = index.stats();

    let view_url = format!("https://pypi-data.github.io/website/repositories/pypi-mirror-{}", index.index());
    let code_url = format!("https://github.com/pypi-data/pypi-mirror-{}/tree/code/packages", index.index());

    let context = Context {
        name: format!("PyPI code {}", index.index()),
        total_packages: stats.total_packages,
        first_package_time: format!("{}", stats.earliest_package.format("%Y-%m-%d %H:%M")),
        last_package_time: format!("{}", stats.latest_package.format("%Y-%m-%d %H:%M")),
        done_count: stats.done_packages,
        percent_done: stats.percent_done(),
        view_url,
        code_url
    };

    let rendered = tt.render("readme", &context)?;
    Ok(rendered)
}
