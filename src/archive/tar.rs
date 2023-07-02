use crate::archive::content::get_contents;
use crate::archive::{skip_archive_entry, ArchiveItem, ExtractionError};
use crate::data::IndexItem;
use bzip2::read::BzDecoder;
use flate2::read::GzDecoder;
use std::io::{BufReader, Read};
use tar::Archive;

// I don't know how to generalise these.
pub fn iter_tar_gz_contents(
    archive: &mut Archive<GzDecoder<BufReader<Box<dyn Read + Send + Sync>>>>,
) -> impl Iterator<Item = Result<(IndexItem, Option<ArchiveItem>), ExtractionError>> + '_ {
    archive
        .entries()
        .unwrap()
        .flatten()
        .filter_map(|mut entry| {
            let path = entry.path().unwrap().to_str().unwrap().to_string();
            let size = entry.size();
            let (index_item, data) = match get_contents(size as usize, &mut entry) {
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
            if skip_archive_entry(&path, size) {
                return Some(Ok((index_item, None)));
            }
            let item = ArchiveItem { path, size, data };
            Some(Ok((index_item, Some(item))))
        })
}

pub fn iter_tar_bz_contents(
    archive: &mut Archive<BzDecoder<BufReader<Box<dyn Read + Send + Sync>>>>,
) -> impl Iterator<Item = Result<(IndexItem, Option<ArchiveItem>), ExtractionError>> + '_ {
    archive
        .entries()
        .unwrap()
        .flatten()
        .filter_map(|mut entry| {
            let path = entry.path().unwrap().to_str().unwrap().to_string();
            let size = entry.size();
            let (index_item, data) = match get_contents(size as usize, &mut entry) {
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
                    // panic!("Error inspecting content!! {e}")
                }
            };
            if skip_archive_entry(&path, size) {
                return Some(Ok((index_item, None)));
            }
            let item = ArchiveItem { path, size, data };
            Some(Ok((index_item, Some(item))))
        })
}
