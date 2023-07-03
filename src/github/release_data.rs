use crate::github::{get_client, GithubError};
use flate2::write::GzDecoder;
use graphql_client::{GraphQLQuery, Response};
use indicatif::{ProgressBar, ProgressStyle};
use std::fs::File;
use std::io;
use std::io::{BufWriter, Read};
use std::path::Path;
use std::time::Duration;
use url::Url;

type URI = Url;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "src/github/graphql/schema.graphql",
    query_path = "src/github/graphql/latest_release.graphql",
    response_derives = "Debug"
)]
pub struct GetLatestRelease;

pub fn download_pypi_data_release(
    token: &str,
    to_file: &Path,
    progress: bool,
) -> Result<(), GithubError> {
    let variables = get_latest_release::Variables {};
    let request_body = GetLatestRelease::build_query(variables);

    let client = get_client();

    let response = client
        .post("https://api.github.com/graphql")
        .set("Authorization", &format!("bearer {token}"))
        .send_json(request_body)?;

    let body: Response<get_latest_release::ResponseData> = response.into_json()?;
    let asset_url = body
        .data
        .and_then(|d| d.repository)
        .and_then(|r| r.latest_release)
        .map(|a| a.release_assets)
        .and_then(|n| n.nodes)
        .and_then(|r| r.into_iter().flatten().next())
        .map(|n| n.url)
        .ok_or(GithubError::InvalidResponse)?;

    let download_response = ureq::request_url("GET", &asset_url).call()?;
    let content_length: Option<u64> = download_response
        .header("Content-Length")
        .and_then(|v| v.parse().ok());
    let response_reader = download_response.into_reader();

    let mut reader: Box<dyn Read> = if progress {
        let bar = match content_length {
            None => ProgressBar::new_spinner(),
            Some(length) => ProgressBar::new(length)
                .with_style(ProgressStyle::with_template("{percent}% (eta {eta}) {bar:40.cyan/blue}  {bytes} of {total_bytes}. {binary_bytes_per_sec}").unwrap()),
        };
        bar.enable_steady_tick(Duration::from_millis(250));
        Box::new(bar.wrap_read(response_reader))
    } else {
        response_reader
    };

    let output_file = File::create(to_file)?;
    let mut writer = GzDecoder::new(BufWriter::new(output_file));
    io::copy(&mut reader, &mut writer)?;

    Ok(())
}
