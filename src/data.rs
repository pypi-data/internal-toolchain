use parquet::{
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::parser::parse_message_type,
};
use parquet_derive::ParquetRecordWriter;
use rusqlite::{Connection, Result};
use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::archive::content::ContentType;
use crate::repository::package::RepositoryPackage;
use chrono::prelude::*;
use itertools::Itertools;
use parquet::basic::{Compression, Encoding};
use parquet::record::RecordWriter;

pub fn get_ordered_packages_since(
    db: &PathBuf,
    since: DateTime<Utc>,
    limit: Option<usize>,
) -> Result<Vec<RepositoryPackage>> {
    let conn = Connection::open(db)?;
    let mut stmt = conn.prepare(
        "SELECT projects.name, \
                    projects.version, \
                    url, \
                    upload_time \
              FROM urls \
              join projects on urls.project_id = projects.id \
              where upload_time > ?1\
              order by upload_time ASC",
    )?;
    let urls_iter = stmt
        .query_map([since], |row| {
            Ok(RepositoryPackage {
                project_name: row.get(0)?,
                project_version: row.get(1)?,
                url: row.get(2)?,
                upload_time: row.get(3)?,
                processed: false,
            })
        })?
        .map(|v| v.unwrap());

    match limit {
        None => Ok(urls_iter.collect()),
        Some(limit) => Ok(urls_iter.take(limit).collect()),
    }
}

pub struct IndexItem {
    pub path: String,
    pub size: u64,
    pub content_type: ContentType,
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
    pub content_type: &'static str,
}

pub struct RepositoryFileIndexWriter {
    writer: Option<SerializedFileWriter<File>>,
}

impl RepositoryFileIndexWriter {
    pub fn new(path: &Path) -> Mutex<Self> {
        let message_type = "
            message schema {
                REQUIRED BINARY project_name (UTF8);
                REQUIRED BINARY project_version (UTF8);
                REQUIRED INT64 uploaded_on (TIMESTAMP_MILLIS);
                REQUIRED BINARY path (UTF8);
                REQUIRED INT64 size;
                REQUIRED BINARY content_type (UTF8);
            }
        ";
        let schema = Arc::new(parse_message_type(message_type).unwrap());
        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .set_dictionary_enabled(true)
                .set_column_dictionary_enabled("path".into(), false)
                .set_column_dictionary_enabled("size".into(), false)
                .set_column_encoding("path".into(), Encoding::DELTA_BYTE_ARRAY)
                .build(),
        );
        let file = fs::File::create(path).unwrap();
        let writer = SerializedFileWriter::new(file, schema, props).unwrap();
        Mutex::new(RepositoryFileIndexWriter {
            writer: Some(writer),
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
            .map(|v| RepositoryFileIndexItem {
                project_name: &index.package.project_name,
                project_version: &index.package.project_version,
                uploaded_on: index.package.upload_time.timestamp(),
                path: v.path,
                size: v.size,
                content_type: v.content_type.into(),
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
