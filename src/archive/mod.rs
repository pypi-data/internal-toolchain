pub mod content;
pub mod tar;
pub mod zip;

use std::fmt::{Display, Formatter};
use std::io;

use std::str::FromStr;
use thiserror::Error;

pub const KB: u64 = 1024;
pub const MB: u64 = 1024 * KB;
pub const MAX_FILE_SIZE: u64 = 5 * MB;

#[derive(Error, Debug)]
pub enum ExtractionError {
    #[error("IO Error: {0}")]
    IOError(#[from] io::Error),
}

#[derive(Debug, PartialEq)]
pub enum ArchiveType {
    Zip,
    TarGz,
    TarBz,
}

impl FromStr for ArchiveType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "egg" | "zip" | "whl" | "exe" => Ok(ArchiveType::Zip),
            "gz" => Ok(ArchiveType::TarGz),
            "bz2" => Ok(ArchiveType::TarBz),
            _ => Err(()),
        }
    }
}

pub struct ArchiveItem {
    pub path: String,
    size: u64,
    pub data: Vec<u8>,
}

impl Display for ArchiveItem {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ArchiveItem {}. size {} kb / {} bytes)",
            self.path,
            self.size / KB,
            self.size
        )
    }
}

pub fn skip_archive_entry(name: &str, size: u64) -> bool {
    if !(1..=MAX_FILE_SIZE).contains(&size) {
        return true;
    }

    if !name.ends_with(".py") {
        return true;
    }
    if name.contains("/venv/") || name.contains("/.venv/") {
        return true;
    }
    false
}
