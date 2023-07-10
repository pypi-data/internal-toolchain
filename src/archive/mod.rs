pub mod content;
pub mod tar;
pub mod zip;

use std::fmt::{Display, Formatter};
use std::io;

use crate::archive::content::KB;
use ::zip::result::ZipError;
use std::str::FromStr;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ExtractionError {
    #[error("IO Error: {0}")]
    IOError(#[from] io::Error),

    #[error("Zip Error: {0}")]
    ZipError(#[from] ZipError),
}

#[derive(Debug, PartialEq)]
pub enum ArchiveType {
    Zip,
    TarGz,
    TarBz,
    Exe,
}

impl FromStr for ArchiveType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "egg" | "zip" | "whl" => Ok(ArchiveType::Zip),
            "gz" => Ok(ArchiveType::TarGz),
            "bz2" => Ok(ArchiveType::TarBz),
            "exe" => Ok(ArchiveType::Exe),
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
            self.size as usize / KB,
            self.size
        )
    }
}
