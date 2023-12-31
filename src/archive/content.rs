use std::cmp::min;
use std::io;
use std::io::{BufRead, ErrorKind, Read};

use content_inspector::{inspect, ContentType as InspectType};
use git2::{ObjectType, Oid};
use lazy_regex::regex_is_match;

pub const KB: usize = 1024;
pub const MB: usize = 1024 * KB;
pub const MAX_PYTHON_SIZE: usize = 5 * MB;
pub const MAX_NON_PYTHON_SIZE: usize = 200 * KB;

#[derive(Copy, Clone, Debug)]
pub enum SkipReason {
    Binary,
    LongLines,
    TooLarge,
    Empty,
    VersionControlSystem,
    Venv,
}

impl From<SkipReason> for &'static str {
    fn from(val: SkipReason) -> Self {
        match val {
            SkipReason::Binary => "binary",
            SkipReason::LongLines => "text-long-lines",
            SkipReason::TooLarge => "too-large",
            SkipReason::Empty => "empty",
            SkipReason::VersionControlSystem => "version-control",
            SkipReason::Venv => "virtualenv",
        }
    }
}

pub enum Content {
    Skip {
        path: String,
        archive_path: String,
        hash: [u8; 20],
        reason: SkipReason,
        lines: Option<usize>,
    },
    Add {
        path: String,
        archive_path: String,
        hash: [u8; 20],
        lines: usize,
        contents: Vec<u8>,
    },
}

// pub struct Content {
//     pub path: String,
//     pub contents: Option<Vec<u8>>,
//     pub hash: String,
//     pub content_type: ContentType,
//     pub lines: Option<usize>
// }

pub fn get_contents<R: Read>(
    size: usize,
    reader: &mut R,
    archive_path: String,
    prefix: &str,
) -> io::Result<Content> {
    let mut vec = Vec::with_capacity(size);
    reader.read_to_end(&mut vec)?;

    let max_idx = min(1024, vec.len());
    let content_type = inspect(&vec[..max_idx]);

    let mut path =
        format!("{prefix}{}", &archive_path.replace('\n', "_newline")).replace("//", "/");

    if path.ends_with(".git") {
        path = path.replace(".git", ".git_");
    }

    let path = if path.contains("./") {
        path.replace("./", "")
    } else {
        path
    };

    let oid = Oid::hash_object(ObjectType::Blob, &vec)
        .map_err(|_| io::Error::from(ErrorKind::InvalidInput))?;
    let hash: [u8; 20] = oid.as_bytes()[..].try_into().unwrap();

    if content_type == InspectType::BINARY {
        return Ok(Content::Skip {
            path,
            archive_path,
            hash,
            reason: SkipReason::Binary,
            lines: None,
        });
    }

    if size == 0 {
        return Ok(Content::Skip {
            path,
            archive_path,
            hash,
            reason: SkipReason::Empty,
            lines: Some(0),
        });
    }

    let lines = vec.lines().count();

    // Pyarmor files are just big bundles of bytecode. This isn't helpful and causes
    // large repositories. They appear to always start with this token.
    if vec.starts_with("__pyarmor".as_ref()) {
        return Ok(Content::Skip {
            path,
            archive_path,
            hash,
            reason: SkipReason::Binary,
            lines: Some(lines),
        });
    }
    // Ignore git LFS files
    if vec.starts_with("version https://git-lfs".as_ref()) {
        return Ok(Content::Skip {
            path,
            archive_path,
            hash,
            reason: SkipReason::VersionControlSystem,
            lines: Some(lines),
        });
    }
    // Ignore non-python files above a specific size, and non python files above a different size.
    if path.ends_with(".py") {
        if !(1..=MAX_PYTHON_SIZE).contains(&size) {
            return Ok(Content::Skip {
                path,
                archive_path,
                hash,
                reason: SkipReason::TooLarge,
                lines: Some(lines),
            });
        }
    } else if !(1..=MAX_NON_PYTHON_SIZE).contains(&size) {
        return Ok(Content::Skip {
            path,
            archive_path,
            hash,
            reason: SkipReason::TooLarge,
            lines: Some(lines),
        });
    }

    if regex_is_match!(r#"(^|/)(\.git|\.hg|\.svn)/"#, &path) {
        return Ok(Content::Skip {
            path,
            archive_path,
            hash,
            reason: SkipReason::VersionControlSystem,
            lines: Some(lines),
        });
    }
    // |
    if regex_is_match!(r#"(^|/)(\.venv|venv|site-packages)/"#, &path) {
        return Ok(Content::Skip {
            path,
            archive_path,
            hash,
            reason: SkipReason::Venv,
            lines: Some(lines),
        });
    }

    // The areixio package contains very large python files that contain some kind of obfuscated
    // bytecode. We skip these, and potentially others in general, by detecting if the file have
    // very few lines but are comparatively large.
    let total_lines = vec.iter().filter(|v| **v == b'\n').take(5).count();
    if total_lines < 5 && size >= (50 * KB) {
        return Ok(Content::Skip {
            path,
            archive_path,
            hash,
            reason: SkipReason::LongLines,
            lines: Some(lines),
        });
    }

    Ok(Content::Add {
        path,
        archive_path,
        hash,
        lines,
        contents: vec,
    })
}
