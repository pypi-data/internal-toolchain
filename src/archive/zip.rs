use crate::archive::content::get_contents;
use crate::archive::{skip_archive_entry, ArchiveItem};
use crate::data::IndexItem;
use std::io::{BufReader, Read};
use zip::read::read_zipfile_from_stream;

pub fn iter_zip_package_contents(
    reader: &mut BufReader<Box<dyn Read + Send + Sync>>,
) -> Option<(IndexItem, Option<ArchiveItem>)> {
    while let Some(mut zipfile) = read_zipfile_from_stream(reader).unwrap() {
        if !zipfile.is_file() {
            continue;
        }
        let path = zipfile.name().to_string();
        let size = zipfile.size();
        let (index_item, data) = match get_contents(zipfile.size() as usize, &mut zipfile) {
            Ok((None, content_type)) => {
                return Some((
                    IndexItem {
                        path,
                        size,
                        content_type,
                    },
                    None,
                ));
            }
            Ok((Some(v), content_type)) => (
                IndexItem {
                    path: path.clone(),
                    size,
                    content_type,
                },
                v,
            ),
            Err(e) => {
                panic!("Error inspecting content!! {e}")
            }
        };

        if skip_archive_entry(zipfile.name(), zipfile.size()) {
            return Some((index_item, None));
        }

        let item = ArchiveItem { path, size, data };
        return Some((index_item, Some(item)));
    }
    None
}
