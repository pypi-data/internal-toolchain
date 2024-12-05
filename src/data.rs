use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};

use itertools::Itertools;
use polars::prelude::*;

use crate::archive::content::SkipReason;
use crate::repository::package::RepositoryPackage;

#[derive(Debug)]
pub struct IndexItem {
    pub path: String,
    pub archive_path: String,
    pub size: u64,
    pub hash: [u8; 20],
    pub skip_reason: Option<SkipReason>,
    pub lines: Option<usize>,
}

#[derive(Debug)]
pub struct PackageFileIndex<'a> {
    pub package: &'a RepositoryPackage,
    pub items: Vec<IndexItem>,
}

impl<'a> PackageFileIndex<'a> {
    pub fn new(package: &'a RepositoryPackage, items: Vec<IndexItem>) -> Self {
        PackageFileIndex { package, items }
    }

    pub fn into_dataframe(self) -> DataFrame {
        let release = self.package.package_filename();
        let upload_time = self.package.upload_time.naive_utc();
        let skip_series = Series::new(
            "skip_reason".into(),
            self.items
                .iter()
                .map(|x| {
                    let str_value: &'static str =
                        x.skip_reason.map(|s| s.into()).unwrap_or_default();
                    str_value
                })
                .collect_vec(),
        );
        let series = vec![
            Series::new(
                "project_name".into(),
                self.items
                    .iter()
                    .map(|_| self.package.project_name.as_str())
                    .collect_vec(),
            ),
            Series::new(
                "project_version".into(),
                self.items
                    .iter()
                    .map(|_| self.package.project_version.as_str())
                    .collect_vec(),
            ),
            Series::new(
                "project_release".into(),
                self.items.iter().map(|_| release).collect_vec(),
            ),
            DatetimeChunked::from_naive_datetime(
                "uploaded_on".into(),
                self.items.iter().map(|_| upload_time).collect_vec(),
                TimeUnit::Milliseconds,
            )
            .into_series(),
            Series::new(
                "path".into(),
                self.items.iter().map(|x| x.path.as_str()).collect_vec(),
            ),
            Series::new(
                "archive_path".into(),
                self.items
                    .iter()
                    .map(|x| x.archive_path.as_str())
                    .collect_vec(),
            ),
            Series::new(
                "size".into(),
                self.items.iter().map(|x| x.size).collect_vec(),
            ),
            Series::new(
                "hash".into(),
                self.items.iter().map(|x| x.hash.to_vec()).collect_vec(),
            ),
            skip_series,
            Series::new(
                "lines".into(),
                self.items
                    .iter()
                    .map(|x| (x.lines.unwrap_or_default()) as u64)
                    .collect_vec(),
            ),
        ];
        DataFrame::new(series.into_iter().map(Column::Series).collect()).unwrap()
    }
}

pub struct RepositoryFileIndexWriter {
    path: PathBuf,
    dataframe: Option<DataFrame>,
}

impl RepositoryFileIndexWriter {
    pub fn new(path: &Path) -> Self {
        Self {
            dataframe: None,
            path: path.into(),
        }
    }

    pub fn write_index(&mut self, index: PackageFileIndex) {
        let df = index.into_dataframe();
        match &mut self.dataframe {
            None => self.dataframe = Some(df),
            Some(other_df) => {
                other_df.vstack_mut(&df).unwrap();
            }
        }
    }

    pub fn finish(self) -> anyhow::Result<()> {
        let mut df = self.dataframe.unwrap();
        df.sort_in_place(
            ["path"],
            SortMultipleOptions::new()
                .with_multithreaded(true)
                .with_order_descending(true),
        )?;
        let w = File::create(self.path)?;
        let writer = ParquetWriter::new(BufWriter::new(w))
            .with_statistics(StatisticsOptions::full())
            .with_compression(ParquetCompression::Zstd(Some(ZstdLevel::try_new(12)?)));
        writer.finish(&mut df)?;
        Ok(())
    }
}

pub fn merge_parquet_files(
    input_path: &Path,
    output_path: &Path,
    repo_id: usize,
) -> Result<(), anyhow::Error> {
    let mut df = LazyFrame::scan_parquet(
        input_path.join("*.parquet").to_str().unwrap(),
        Default::default(),
    )?;
    df = df.sort(
        ["path"],
        SortMultipleOptions::new()
            .with_order_descending(true)
            .with_nulls_last(false)
            .with_multithreaded(true)
            .with_maintain_order(false),
    );
    df = df.with_column(lit(repo_id as u32).alias("repository").cast(DataType::UInt32));
    let mut df = df.collect()?;
    let w = File::create(output_path)?;
    let writer = ParquetWriter::new(BufWriter::new(w))
        .with_statistics(StatisticsOptions::full())
        .with_compression(ParquetCompression::Zstd(Some(ZstdLevel::try_new(12)?)));
    writer.finish(&mut df)?;
    Ok(())
}
