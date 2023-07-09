use std::cell::RefCell;
use anyhow::Context;
use indicatif::{ParallelProgressIterator, ProgressIterator};
use parquet::{
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::parser::parse_message_type,
};
use parquet_derive::ParquetRecordWriter;
use rayon::prelude::*;
use rusqlite::Result;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::ops::DerefMut;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use thread_local::ThreadLocal;

use crate::archive::content::ContentType;
use crate::repository::package::RepositoryPackage;
use itertools::Itertools;
use parquet::arrow::arrow_reader::ArrowReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};

use parquet::record::RecordWriter;
use parquet::schema::types::Type;

pub struct IndexItem {
    pub path: String,
    pub size: u64,
    pub hash: String,
    pub content_type: ContentType,
    pub lines: Option<usize>,
}

pub struct PackageFileIndex<'a> {
    pub package: &'a RepositoryPackage,
    pub items: Vec<IndexItem>,
}

impl<'a> PackageFileIndex<'a> {
    pub fn new(package: &'a RepositoryPackage, items: Vec<IndexItem>) -> Self {
        PackageFileIndex { package, items }
    }
}

#[derive(ParquetRecordWriter)]
struct RepositoryFileIndexItem<'a> {
    pub project_name: &'a str,
    pub project_version: &'a str,
    pub uploaded_on: i64,
    pub path: String,
    pub size: u64,
    pub hash: String,
    pub content_type: &'static str,
    pub lines: usize,
    // pub github_repo: usize,
}

pub struct RepositoryFileIndexWriter {
    writer: Option<SerializedFileWriter<File>>,
    github_repo: usize,
}

fn get_arrow_schema_and_props(batch_size: usize) -> (Arc<Type>, Arc<WriterProperties>) {
    let message_type = "
            message schema {
                REQUIRED BINARY project_name (UTF8);
                REQUIRED BINARY project_version (UTF8);
                REQUIRED INT64 uploaded_on (TIMESTAMP_MILLIS);
                REQUIRED BINARY path (UTF8);
                REQUIRED INT64 size;
                REQUIRED BINARY hash (UTF8);
                REQUIRED BINARY content_type (UTF8);
                REQUIRED INT64 lines;
            }
        ";
    let schema = Arc::new(parse_message_type(message_type).unwrap());
    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(12).unwrap()))
            .set_write_batch_size(batch_size)
            .set_data_page_row_count_limit(batch_size)
            .set_max_row_group_size(batch_size)
            .set_data_page_size_limit(1024 * 1024 * 1024)
            .set_column_dictionary_enabled("path".into(), false)
            .set_column_dictionary_enabled("size".into(), false)
            .set_column_dictionary_enabled("lines".into(), false)
            .set_column_dictionary_enabled("hash".into(), false)
            .set_column_dictionary_enabled("uploaded_on".into(), false)
            .set_column_encoding("path".into(), Encoding::PLAIN)
            .set_column_encoding("uploaded_on".into(), Encoding::PLAIN)
            .build(),
    );
    (schema, props)
}

impl RepositoryFileIndexWriter {
    pub fn new(path: &Path, github_repo: usize) -> Mutex<Self> {
        let (schema, props) = get_arrow_schema_and_props(1024 * 1024);
        let file = fs::File::create(path).unwrap();
        let writer = SerializedFileWriter::new(file, schema, props).unwrap();
        Mutex::new(RepositoryFileIndexWriter {
            writer: Some(writer),
            github_repo
        })
    }

    pub fn write_index(&mut self, index: PackageFileIndex) {
        let writer = match &mut self.writer {
            None => panic!("IndexWriter closed!"),
            Some(w) => w,
        };
        let mut row_group = writer.next_row_group().unwrap();
        let mut chunks = index
            .items
            .into_iter()
            .sorted_by(|v1, v2| v1.path.cmp(&v2.path))
            .map(|v| RepositoryFileIndexItem {
                project_name: &index.package.project_name,
                project_version: &index.package.project_version,
                uploaded_on: index.package.upload_time.timestamp_millis(),
                path: v.path,
                size: v.size,
                hash: v.hash,
                content_type: v.content_type.into(),
                lines: v.lines.unwrap_or_default(),
                // github_repo: self.github_repo,
            })
            .collect_vec();

        chunks.sort_by(|c1, c2| c1.path.cmp(&c2.path));

        (&chunks[..]).write_to_row_group(&mut row_group).unwrap();
        row_group.close().unwrap();
    }
}

impl Drop for RepositoryFileIndexWriter {
    fn drop(&mut self) {
        let writer = self.writer.take();
        if let Some(w) = writer {
            w.close().unwrap();
        }
    }
}

pub fn merge_parquet_files(
    files: Vec<PathBuf>,
    output_file: PathBuf,
    batch_size: usize,
) -> anyhow::Result<()> {
    let (_, props) = get_arrow_schema_and_props(batch_size);
    let reader = ArrowReaderBuilder::try_new(File::open(&files[0]).unwrap())?;
    let mut writer = ArrowWriter::try_new(
        File::create(output_file).unwrap(),
        (*reader.schema()).clone(),
        Some((*props).clone()),
    )
    .unwrap();
    for file in files {
        let reader = ArrowReaderBuilder::try_new(File::open(file).unwrap())?;
        for batch in reader.with_batch_size(batch_size).build()? {
            writer.write(&batch?)?;
        }
    }
    writer.close()?;
    Ok(())
}

fn merge_parquet_file(
    file: &PathBuf,
    writer: &mut ArrowWriter<File>,
    batch_size: usize,
) -> anyhow::Result<()> {
    let reader = ArrowReaderBuilder::try_new(File::open(&file)?)
        .with_context(|| format!("File: {}", file.display()))?;
    for batch in reader.with_batch_size(batch_size).build()? {
        writer.write(&batch?)?;
    }
    Ok(())
}

pub fn reduce_parquet_files(
    files: Vec<PathBuf>,
    output_dir: PathBuf,
    batch_size: usize,
) -> anyhow::Result<()> {
    let (_, props) = get_arrow_schema_and_props(batch_size);
    let reader = ArrowReaderBuilder::try_new(File::open(&files[0])?)?;
    let schema = reader.schema();

    let tls: ThreadLocal<_> = ThreadLocal::with_capacity(rayon::max_num_threads());

    let results: Vec<anyhow::Result<_>> = files
        .into_par_iter()
        .progress()
        .map_init(
            || {
                tls.get_or(|| {
                    let idx = rayon::current_thread_index().unwrap();
                    let output = &output_dir.join(format!("part-{idx}.parquet"));
                    let writer = ArrowWriter::try_new(
                        File::create(output).unwrap(),
                        schema.clone(),
                        Some((*props).clone()),
                    )
                    .unwrap();
                    RefCell::new(writer)
                })
            },
            |writer, path| {
                merge_parquet_file(&path, &mut writer.borrow_mut(), batch_size)
                    .with_context(|| format!("Failed to merge file: {}", path.display()))
            },
        )
        .collect();

    for writer in tls.into_iter() {
        writer.into_inner().close()?;
    }

    for result in results {
        result?;
    }
    Ok(())
}
