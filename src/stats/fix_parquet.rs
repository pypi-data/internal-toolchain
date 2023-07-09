use polars::prelude::ParquetCompression::Zstd;
use polars::prelude::*;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;

pub fn fix_index_file(path: &Path) -> anyhow::Result<()> {
    // Unfortunately the content_type of 70% of the indexes is GitLFS when it should be "text".
    // For now we just update this in-place and remove all "git-lfs" references.
    // If the size of the file is 0 the content_type is set to "too-large".
    let mut frame = LazyFrame::scan_parquet(path, Default::default())?
        .select([
            col("*"),
            as_struct(&[col("content_type"), col("size")])
                .map(
                    |series| {
                        let ca = series.struct_()?;
                        let content_type = ca.field_by_name("content_type")?;
                        let size = ca.field_by_name("size")?;
                        let out: Utf8Chunked = content_type
                            .utf8()?
                            .into_iter()
                            .zip(size.i64()?.into_iter())
                            .map(|(content_type, size)| match (content_type, size) {
                                (Some("git-lfs"), _) => Some("text"),
                                (_, Some(0)) => Some("empty"),
                                (_, _) => content_type,
                            })
                            .collect::<Utf8Chunked>();
                        Ok(Some(out.into_series()))
                    },
                    GetOutput::from_type(DataType::Utf8),
                )
                .alias("content_type_fixed"),
        ])
        .drop_columns(["content_type"])
        .rename(["content_type_fixed"], ["content_type"])
        .collect()?;

    let w = File::create(path)?;
    let writer =
        ParquetWriter::new(BufWriter::new(w)).with_compression(Zstd(Some(ZstdLevel::try_new(12)?)));
    writer.finish(&mut frame)?;
    Ok(())
}
