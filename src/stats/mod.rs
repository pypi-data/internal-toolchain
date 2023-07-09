pub mod fix_parquet;

use polars::prelude::*;
use serde::Serialize;
use std::path::Path;

use url::Url;

#[derive(Debug, Serialize)]
pub struct DetailedStats {
    pub total_size: u64,
    pub top_projects: Vec<ProjectStat>,
}

#[derive(Debug, Serialize)]
pub struct ProjectStat {
    name: String,
    total_size: u64,
    text_size: u64,
    total_files: u64,
    unique_files: u64,
}

pub fn parquet_url(name: &str) -> Url {
    format!(
        "https://github.com/pypi-data/{}/releases/download/latest/combined.parquet",
        name
    )
    .parse()
    .unwrap()
}

fn get_dataframe(path: &Path) -> anyhow::Result<LazyFrame> {
    let frame =
        LazyFrame::scan_parquet(path.join("*.parquet").to_str().unwrap(), Default::default())?;
    Ok(frame)
}

pub fn count(path: &Path) -> anyhow::Result<()> {
    let frame = get_dataframe(path)?;
    let aggregate_stats = frame
        .select([col("lines").sum(), col("size").sum()])
        .collect()
        .unwrap();
    println!("{:?}", aggregate_stats);
    Ok(())
}

impl DetailedStats {
    // pub fn get_detailed_stats(&self, client: Agent) -> Option<DetailedStats> {
    //     use std::fs::File;
    //     use std::io::BufWriter;
    //
    //     let tmp_dir = tempdir::TempDir::new("status").unwrap();
    //     let parquet_path = tmp_dir.path().join("combined.parquet");
    //     let request = client.get(self.parquet_url().as_ref()).call();
    //     let mut reader = match request {
    //         Ok(r) => r.into_reader(),
    //         Err(ureq::Error::Status(404, _)) => return None,
    //         Err(e) => panic!("{:?}", e),
    //     };
    //     let mut output = BufWriter::new(File::create(&parquet_path).unwrap());
    //     std::io::copy(&mut reader, &mut output).unwrap();
    //
    //     let df = LazyFrame::scan_parquet(&parquet_path, Default::default()).unwrap();
    //     let binding = df
    //         .groupby([col("project_name")])
    //         .agg([
    //             col("size").sum().alias("size").cast(DataType::UInt64),
    //             col("size")
    //                 .filter(col("content_type").eq(lit("text")))
    //                 .sum()
    //                 .alias("text_size")
    //                 .cast(DataType::UInt64),
    //             col("hash")
    //                 .count()
    //                 .alias("total_files")
    //                 .cast(DataType::UInt64),
    //             col("hash")
    //                 .unique()
    //                 .count()
    //                 .alias("unique_files")
    //                 .cast(DataType::UInt64),
    //         ])
    //         .sort(
    //             "text_size",
    //             SortOptions {
    //                 descending: true,
    //                 nulls_last: true,
    //                 multithreaded: true,
    //             },
    //         )
    //         .limit(5)
    //         .collect()
    //         .unwrap();
    //     let df2 = LazyFrame::scan_parquet(&parquet_path, Default::default()).unwrap();
    //     let binding2 = df2
    //         .select(&[
    //             sum("size").alias("total_size").cast(DataType::UInt64),
    //             // col("hash")
    //             //     .sort_by([col("size").rank(Default::default(), None)], [false])
    //             //     .last()
    //             //     .alias("largest_file"),
    //         ])
    //         .collect()
    //         .unwrap();
    //
    //     let total_size = binding2.select_series(["total_size"]).unwrap()[0]
    //         .u64()
    //         .unwrap()
    //         .get(0)
    //         .unwrap();
    //
    //     // let x = binding.get_row(0).unwrap();
    //     // println!("{x:?}");
    //     let mut top_projects = vec![];
    //     let series = binding
    //         .select_series([
    //             "project_name",
    //             "size",
    //             "text_size",
    //             "total_files",
    //             "unique_files",
    //         ])
    //         .unwrap();
    //     for (name, size, text_size, total_files, unique_files) in itertools::multizip((
    //         series[0].utf8().unwrap(),
    //         series[1].u64().unwrap(),
    //         series[2].u64().unwrap(),
    //         series[3].u64().unwrap(),
    //         series[4].u64().unwrap(),
    //     )) {
    //         top_projects.push(ProjectStat {
    //             name: name.unwrap().to_string(),
    //             total_size: size.unwrap(),
    //             // total_size_human: format_size(size.unwrap(), DECIMAL),
    //             text_size: text_size.unwrap(),
    //             // text_size_human: format_size(text_size.unwrap(), DECIMAL),
    //             total_files: total_files.unwrap(),
    //             unique_files: unique_files.unwrap(),
    //         });
    //         // println!("{name:?} {size:?} {total_files:?} {unique_files:?}");
    //     }
    //     // let x = binding.get_row(0).unwrap();
    //     // println!("{x:?}");
    //     // let foo = match x.0.get(0).unwrap() {
    //     //     AnyValue::UInt64(v) => { *v }
    //     //     _ => unreachable!(),
    //     // };
    //
    //     Some(DetailedStats {
    //         total_size,
    //         // total_size_human: format_size(total_size, DECIMAL),
    //         top_projects,
    //     })
    // }
}
