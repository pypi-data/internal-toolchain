use crate::archive::content::{get_contents, Content};
use crate::archive::{ArchiveItem, ExtractionError};
use crate::data::IndexItem;
use anyhow::Result;
use bzip2::read::BzDecoder;
use flate2::read::GzDecoder;
use std::io;
use std::io::{BufReader, Read};
use tar::Archive;

fn get_path<T: io::Read>(entry: &tar::Entry<T>) -> Option<String> {
    entry.path().ok()?.to_str().map(|s| s.to_string())
}

// I don't know how to generalise these.
pub fn iter_tar_gz_contents(
    archive: &mut Archive<GzDecoder<BufReader<Box<dyn Read + Send + Sync>>>>,
    prefix: String,
) -> io::Result<impl Iterator<Item = Result<(IndexItem, Option<ArchiveItem>), ExtractionError>> + '_>
{
    let result = archive.entries()?.flatten().filter_map(move |mut entry| {
        let path = get_path(&entry)?;
        let size = entry.size();
        if path.ends_with('/') {
            return None;
        }
        let (index_item, data) = match get_contents(size as usize, &mut entry, path, &prefix) {
            Ok(Content::Skip {
                path,
                hash,
                content_type,
                lines,
            }) => {
                return Some(Ok((
                    IndexItem {
                        path,
                        size,
                        hash,
                        content_type,
                        lines,
                    },
                    None,
                )));
            }
            Ok(Content::Add {
                path,
                hash,
                content_type,
                lines,
                contents,
            }) => (
                IndexItem {
                    path,
                    size,
                    hash,
                    content_type,
                    lines: Some(lines),
                },
                contents,
            ),
            Err(e) => return Some(Err(ExtractionError::IOError(e))),
        };
        let item = ArchiveItem {
            path: index_item.path.clone(),
            size,
            data,
        };
        Some(Ok((index_item, Some(item))))
    });
    Ok(result)
}

pub fn iter_tar_bz_contents(
    archive: &mut Archive<BzDecoder<BufReader<Box<dyn Read + Send + Sync>>>>,
    prefix: String,
) -> io::Result<impl Iterator<Item = Result<(IndexItem, Option<ArchiveItem>), ExtractionError>> + '_>
{
    let result = archive.entries()?.flatten().filter_map(move |mut entry| {
        let path = get_path(&entry)?;
        let size = entry.size();
        if path.ends_with('/') {
            return None;
        }
        let (index_item, data) = match get_contents(size as usize, &mut entry, path, &prefix) {
            Ok(Content::Skip {
                path,
                hash,
                content_type,
                lines,
            }) => {
                return Some(Ok((
                    IndexItem {
                        path,
                        size,
                        hash,
                        content_type,
                        lines,
                    },
                    None,
                )));
            }
            Ok(Content::Add {
                path,
                hash,
                content_type,
                lines,
                contents,
            }) => (
                IndexItem {
                    path,
                    size,
                    hash,
                    content_type,
                    lines: Some(lines),
                },
                contents,
            ),
            Err(e) => {
                return Some(Err(ExtractionError::IOError(e)));
            }
        };
        let item = ArchiveItem {
            path: index_item.path.clone(),
            size,
            data,
        };
        Some(Ok((index_item, Some(item))))
    });
    Ok(result)
}
