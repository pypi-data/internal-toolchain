use crate::archive::content::get_contents;
use crate::archive::{ArchiveItem, ExtractionError};
use crate::data::IndexItem;
use std::io::{BufReader, Read};
use zip::read::read_zipfile_from_stream;

pub fn iter_zip_package_contents(
    reader: &mut BufReader<Box<dyn Read + Send + Sync>>,
    prefix: String,
) -> Option<Result<(IndexItem, Option<ArchiveItem>), ExtractionError>> {
    loop {
        return match read_zipfile_from_stream(reader) {
            Ok(Some(mut zipfile)) => {
                if !zipfile.is_file() {
                    continue;
                }
                let path = zipfile.name().to_string();
                let size = zipfile.size();
                if !zipfile.is_file() {
                    continue;
                }
                let (index_item, data) =
                    match get_contents(zipfile.size() as usize, &mut zipfile, &path, &prefix) {
                        Ok((path, None, hash, content_type)) => {
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
                        Ok((path, Some(v), hash, content_type)) => (
                            IndexItem {
                                path,
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
            }
            Ok(None) => None,
            Err(e) => Some(Err(ExtractionError::ZipError(e))),
        };
    }
}
