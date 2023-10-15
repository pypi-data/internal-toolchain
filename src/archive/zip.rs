use crate::archive::content::{get_contents, Content};
use crate::archive::{ArchiveItem, ExtractionError};
use crate::data::IndexItem;
use std::io::BufReader;

use zip::read::read_zipfile_from_stream;

pub fn iter_zip_package_contents<'a>(
    reader: &'a mut BufReader<&'a [u8]>,
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
                    match get_contents(zipfile.size() as usize, &mut zipfile, path, &prefix) {
                        Ok(Content::Skip {
                            path,
                            archive_path,
                            hash,
                            reason,
                            lines,
                        }) => {
                            return Some(Ok((
                                IndexItem {
                                    path,
                                    archive_path,
                                    size,
                                    hash,
                                    skip_reason: Some(reason),
                                    lines,
                                },
                                None,
                            )));
                        }
                        Ok(Content::Add {
                            path,
                            archive_path,
                            hash,
                            lines,
                            contents,
                        }) => (
                            IndexItem {
                                path,
                                archive_path,
                                size,
                                hash,
                                skip_reason: None,
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
            }
            Ok(None) => None,
            Err(e) => Some(Err(ExtractionError::ZipError(e))),
        };
    }
}
