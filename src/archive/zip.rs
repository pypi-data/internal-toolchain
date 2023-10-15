use crate::archive::content::{get_contents, Content};
use crate::archive::{ArchiveItem, ExtractionError};
use crate::data::IndexItem;
use std::io;
use std::io::Cursor;

use zip::ZipArchive;

pub fn iter_zip_contents<'a>(
    zip_archive: &'a mut ZipArchive<Cursor<&'a [u8]>>,
    prefix: String,
) -> io::Result<impl Iterator<Item = Result<(IndexItem, Option<ArchiveItem>), ExtractionError>> + 'a>
{
    let result = (0..zip_archive.len()).filter_map(move |id| {
        let file = zip_archive.by_index(id);
        return match file {
            Ok(mut zipfile) => {
                if !zipfile.is_file() {
                    return None;
                }
                let path = zipfile.name().to_string();
                let size = zipfile.size();
                if !zipfile.is_file() {
                    return None;
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
            Err(e) => Some(Err(ExtractionError::ZipError(e))),
        };
    });
    Ok(result)
}
