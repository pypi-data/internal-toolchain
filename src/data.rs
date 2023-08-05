use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};

use itertools::Itertools;
use polars_core::prelude::*;
use polars_io::prelude::ParquetCompression::Zstd;
use polars_io::prelude::*;
use polars_lazy::prelude::*;

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
        let hash_series = Series::from_any_values_and_dtype(
            "hash",
            &self
                .items
                .iter()
                .map(|x| AnyValue::BinaryOwned(x.hash.to_vec()))
                .collect_vec(),
            &DataType::Array(Box::new(DataType::Binary), 20),
            true,
        )
        .unwrap();
        let series = vec![
            Series::new(
                "project_name",
                self.items
                    .iter()
                    .map(|_| self.package.project_name.as_str())
                    .collect_vec(),
            ),
            Series::new(
                "project_version",
                self.items
                    .iter()
                    .map(|_| self.package.project_version.as_str())
                    .collect_vec(),
            ),
            Series::new(
                "project_release",
                self.items.iter().map(|_| release).collect_vec(),
            ),
            DatetimeChunked::from_naive_datetime(
                "uploaded_on",
                self.items.iter().map(|_| upload_time).collect_vec(),
                TimeUnit::Milliseconds,
            )
            .into_series(),
            Series::new(
                "path",
                self.items.iter().map(|x| x.path.as_str()).collect_vec(),
            ),
            Series::new(
                "archive_path",
                self.items
                    .iter()
                    .map(|x| x.archive_path.as_str())
                    .collect_vec(),
            ),
            Series::new("size", self.items.iter().map(|x| x.size).collect_vec()),
            hash_series,
            Series::new(
                "skip_reason",
                self.items
                    .iter()
                    .map(|x| {
                        let str_value: &'static str =
                            x.skip_reason.map(|s| s.into()).unwrap_or_default();
                        str_value
                    })
                    .collect_vec(),
            ),
            Series::new(
                "lines",
                self.items
                    .iter()
                    .map(|x| (x.lines.unwrap_or_default()) as u64)
                    .collect_vec(),
            ),
        ];
        DataFrame::new(series).unwrap()
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
        match &self.dataframe {
            None => self.dataframe = Some(df),
            Some(other_df) => {
                self.dataframe = Some(other_df.vstack(&df).unwrap());
            }
        }
    }

    pub fn finish(self) -> anyhow::Result<()> {
        let mut df = self.dataframe.unwrap();
        df.sort_in_place(["path"], true, false)?;
        let w = File::create(self.path)?;
        let writer = ParquetWriter::new(BufWriter::new(w))
            .with_statistics(true)
            .with_compression(ParquetCompression::Zstd(Some(ZstdLevel::try_new(12)?)));
        writer.finish(&mut df)?;
        Ok(())
    }
}

pub fn merge_parquet_files(input_path: &Path, output_path: &Path) -> Result<(), anyhow::Error> {
    let mut df = LazyFrame::scan_parquet(
        input_path.join("*.parquet").to_str().unwrap(),
        Default::default(),
    )?;
    df = df.sort(
        "path",
        SortOptions {
            descending: true,
            nulls_last: false,
            multithreaded: true,
            maintain_order: false,
        },
    );
    let mut df = df.collect()?;
    let w = File::create(output_path)?;
    let writer = ParquetWriter::new(BufWriter::new(w))
        .with_statistics(true)
        .with_compression(Zstd(Some(ZstdLevel::try_new(12)?)));
    writer.finish(&mut df)?;
    Ok(())
}
