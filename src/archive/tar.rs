use crate::archive::content::get_contents;
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
) -> io::Result<impl Iterator<Item = Result<(IndexItem, Option<ArchiveItem>), ExtractionError>> + '_>
{
    let result = archive.entries()?.flatten().filter_map(|mut entry| {
        let path = get_path(&entry)?;
        let size = entry.size();
        let (index_item, data) = match get_contents(size as usize, &mut entry, &path) {
            Ok((None, hash, content_type)) => {
                return Some(Ok((
                    IndexItem {
                        path,
                        size,
                        hash,
                        content_type,
                    },
                    None,
                )));
            }
            Ok((Some(v), hash, content_type)) => (
                IndexItem {
                    path: path.clone(),
                    size,
                    hash,
                    content_type,
                },
                v,
            ),
            Err(e) => return Some(Err(ExtractionError::IOError(e))),
        };
        let item = ArchiveItem { path, size, data };
        Some(Ok((index_item, Some(item))))
    });
    Ok(result)
}

pub fn iter_tar_bz_contents(
    archive: &mut Archive<BzDecoder<BufReader<Box<dyn Read + Send + Sync>>>>,
) -> io::Result<impl Iterator<Item = Result<(IndexItem, Option<ArchiveItem>), ExtractionError>> + '_>
{
    let result = archive.entries()?.flatten().filter_map(|mut entry| {
        let path = get_path(&entry)?;
        let size = entry.size();
        let (index_item, data) = match get_contents(size as usize, &mut entry, &path) {
            Ok((None, hash, content_type)) => {
                return Some(Ok((
                    IndexItem {
                        path,
                        size,
                        hash,
                        content_type,
                    },
                    None,
                )));
            }
            Ok((Some(v), hash, content_type)) => (
                IndexItem {
                    path: path.clone(),
                    size,
                    hash,
                    content_type,
                },
                v,
            ),
            Err(e) => {
                return Some(Err(ExtractionError::IOError(e)));
            }
        };
        let item = ArchiveItem { path, size, data };
        Some(Ok((index_item, Some(item))))
    });
    Ok(result)
}
