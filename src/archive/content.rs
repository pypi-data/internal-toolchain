use crate::archive::KB;
use content_inspector::{inspect, ContentType as InspectType};
use data_encoding::HEXUPPER;
use ring::digest::{Context, SHA256};
use std::cmp::min;
use std::io;
use std::io::Read;

pub enum ContentType {
    Text,
    Binary,
    PyArmor,
    GitLFS,
    LongLines,
}

impl From<ContentType> for &'static str {
    fn from(val: ContentType) -> Self {
        match val {
            ContentType::Binary => "binary",
            ContentType::PyArmor => "pyarmor",
            ContentType::GitLFS => "git-lfs",
            ContentType::LongLines => "text-long-lines",
            ContentType::Text => "text",
        }
    }
}

pub fn get_contents<R: Read>(
    size: usize,
    reader: &mut R,
) -> io::Result<(Option<Vec<u8>>, String, ContentType)> {
    // let mut first = [0; 1024];
    // let n = reader.read(&mut first[..])?;
    // let first = &first[..n];
    let mut vec = Vec::with_capacity(size);
    // vec.extend_from_slice(first);
    io::copy(reader, &mut vec)?;

    let max_idx = min(1024, vec.len());
    let content_type = inspect(&vec[..max_idx]);

    let mut context = Context::new(&SHA256);
    context.update(&vec);
    let res = context.finish();
    let hash = HEXUPPER.encode(res.as_ref());

    if content_type == InspectType::BINARY {
        return Ok((None, hash, ContentType::Binary));
    }
    // Pyarmor files are just big bundles of bytecode. This isn't helpful and causes
    // large repositories. They appear to always start with this token.
    if vec.starts_with("__pyarmor".as_ref()) {
        return Ok((None, hash, ContentType::PyArmor));
    }
    // Ignore git LFS files
    if vec.starts_with("version https://git-lfs".as_ref()) {
        return Ok((None, hash, ContentType::GitLFS));
    }

    // The areixio package contains very large python files that contain some kind of obfuscated
    // bytecode. We skip these, and potentially others in general, by detecting if the file have
    // very few lines but are comparatively large.
    let total_lines = vec.iter().filter(|v| **v == b'\n').take(5).count();
    if total_lines < 5 && size >= (50 * KB) as usize {
        return Ok((None, hash, ContentType::LongLines));
    }

    Ok((Some(vec), hash, ContentType::Text))
}
