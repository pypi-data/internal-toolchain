use polars::prelude::*;
use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;

pub fn fix_index_file_content_type(path: &PathBuf) -> anyhow::Result<()> {
    // Unfortunately the content_type of 70% of the indexes is GitLFS when it should be "text".
    // For now we just update this in-place and remove all "git-lfs" references.
    let mut frame = LazyFrame::scan_parquet(&path, Default::default())?.collect()?;
    let frame = frame.apply("content_type", |series| {
        series
            .utf8()
            .unwrap()
            .into_iter()
            .map(|v| {
                v.map(|v| match v {
                    "git-lfs" => "text",
                    _ => v,
                })
            })
            .collect::<Utf8Chunked>()
            .into_series()
    })?;

    let w = File::create(path)?;
    let writer = ParquetWriter::new(BufWriter::new(w));
    writer.finish(frame)?;
    Ok(())
}
