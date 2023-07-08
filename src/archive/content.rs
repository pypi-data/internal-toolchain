use content_inspector::{inspect, ContentType as InspectType};
use data_encoding::HEXLOWER;
use lazy_regex::regex_is_match;
use ring::digest::{Context, SHA256};
use std::cmp::min;
use std::io;
use std::io::{BufRead, Read};

pub const KB: usize = 1024;
pub const MB: usize = 1024 * KB;
pub const MAX_PYTHON_SIZE: usize = 5 * MB;
pub const MAX_NON_PYTHON_SIZE: usize = 100 * KB;

pub enum ContentType {
    Text,
    Binary,
    PyArmor,
    GitLFS,
    LongLines,
    TooLarge,
    Skipped,
}

impl From<ContentType> for &'static str {
    fn from(val: ContentType) -> Self {
        match val {
            ContentType::Binary => "binary",
            ContentType::PyArmor => "pyarmor",
            ContentType::GitLFS => "git-lfs",
            ContentType::LongLines => "text-long-lines",
            ContentType::Text => "text",
            ContentType::TooLarge => "too-large",
            ContentType::Skipped => "skipped",
        }
    }
}

pub enum Content {
    Skip {
        path: String,
        hash: String,
        content_type: ContentType,
        lines: Option<usize>,
    },
    Add {
        path: String,
        hash: String,
        content_type: ContentType,
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
    path: String,
    prefix: &str,
) -> io::Result<Content> {
    let mut vec = Vec::with_capacity(size);
    io::copy(reader, &mut vec)?;

    let max_idx = min(1024, vec.len());
    let content_type = inspect(&vec[..max_idx]);

    let mut context = Context::new(&SHA256);
    context.update(&vec);
    let res = context.finish();
    let hash = HEXLOWER.encode(res.as_ref());

    if content_type == InspectType::BINARY {
        return Ok(Content::Skip {
            path,
            hash,
            content_type: ContentType::Binary,
            lines: None,
        });
    }

    let lines = vec.lines().count();

    // Pyarmor files are just big bundles of bytecode. This isn't helpful and causes
    // large repositories. They appear to always start with this token.
    if vec.starts_with("__pyarmor".as_ref()) {
        return Ok(Content::Skip {
            path,
            hash,
            content_type: ContentType::PyArmor,
            lines: Some(lines),
        });
    }
    // Ignore git LFS files
    if vec.starts_with("version https://git-lfs".as_ref()) {
        return Ok(Content::Skip {
            path,
            hash,
            content_type: ContentType::GitLFS,
            lines: Some(lines),
        });
    }
    // Ignore non-python files above a specific size, and non python files above a different size.
    if path.ends_with(".py") {
        if !(1..=MAX_PYTHON_SIZE).contains(&size) {
            return Ok(Content::Skip {
                path,
                hash,
                content_type: ContentType::TooLarge,
                lines: Some(lines),
            });
        }
    } else if !(1..=MAX_NON_PYTHON_SIZE).contains(&size) {
        return Ok(Content::Skip {
            path,
            hash,
            content_type: ContentType::TooLarge,
            lines: Some(lines),
        });
    }

    if regex_is_match!(
        r#"(^|/)(\.git|\.hg|\.svn|\.venv|venv|site-packages)/"#,
        &path
    ) {
        return Ok(Content::Skip {
            path,
            hash,
            content_type: ContentType::Skipped,
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
            hash,
            content_type: ContentType::LongLines,
            lines: Some(lines),
        });
    }

    let mut path = format!("{prefix}{}", path.replace('\n', "_newline")).replace("//", "/");

    if path.ends_with(".git") {
        path = path.replace(".git", ".git_");
    }

    let path = if path.contains("./") {
        path.replace("./", "")
    } else {
        path
    };

    Ok(Content::Add {
        path,
        hash,
        content_type: ContentType::GitLFS,
        lines,
        contents: vec,
    })
}
