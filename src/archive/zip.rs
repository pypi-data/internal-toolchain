use crate::archive::content::get_contents;
use crate::archive::{skip_archive_entry, ArchiveItem, ExtractionError};
use crate::data::IndexItem;
use std::io::{BufReader, Read};
use zip::read::read_zipfile_from_stream;

pub fn iter_zip_package_contents(
    reader: &mut BufReader<Box<dyn Read + Send + Sync>>,
) -> Option<Result<(IndexItem, Option<ArchiveItem>), ExtractionError>> {
    while let result = read_zipfile_from_stream(reader) {
        match result {
            Ok(Some(mut zipfile)) => {
                if !zipfile.is_file() {
                    continue;
                }
                let path = zipfile.name().to_string();
                let size = zipfile.size();
                let (index_item, data) = match get_contents(zipfile.size() as usize, &mut zipfile) {
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

                if skip_archive_entry(zipfile.name(), zipfile.size()) {
                    return Some(Ok((index_item, None)));
                }

                let item = ArchiveItem { path, size, data };
                return Some(Ok((index_item, Some(item))));
            }
            Ok(None) => {
                return None;
            }
            Err(e) => return Some(Err(ExtractionError::ZipError(e))),
        }
    }
    None
}
