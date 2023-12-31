use chrono::prelude::*;
use std::io;
use std::io::Write;
use std::sync::Mutex;

#[derive(Debug)]
pub struct GitFastImporter<T: Write> {
    output: T,
    current_mark: usize,
    previous_commit_mark: Option<usize>,
    branch: String,
    should_use_from: bool,
    total: usize,
    commit_count: usize,
    skip_contents: bool,
}

impl<T: Write> GitFastImporter<T> {
    pub fn new(
        output: T,
        total: usize,
        branch: String,
        has_code_branch: bool,
        skip_contents: bool,
    ) -> Mutex<Self> {
        Mutex::new(GitFastImporter {
            output,
            current_mark: 0,
            previous_commit_mark: None,
            should_use_from: has_code_branch,
            commit_count: 0,
            total,
            branch,
            skip_contents,
        })
    }

    pub fn finish(&mut self) -> io::Result<()> {
        writeln!(self.output, "done")?;
        Ok(())
    }

    pub fn flush_commit(
        &mut self,
        name: &str,
        paths_to_nodes: Vec<(usize, String)>,
    ) -> io::Result<()> {
        self.current_mark += 1;
        let now = Utc::now();
        writeln!(self.output, "commit refs/heads/{}", self.branch)?;
        writeln!(self.output, "mark :{}", self.current_mark)?;
        writeln!(
            self.output,
            "committer Bot <41898282+github-actions[bot]@users.noreply.github.com> {} +0000",
            now.timestamp()
        )?;

        let commit_message = format!("Add package {name}");
        writeln!(self.output, "data {}", commit_message.len())?;
        writeln!(self.output, "{commit_message}")?;

        if self.should_use_from {
            writeln!(self.output, "from {}", self.branch)?;
            self.should_use_from = false;
        }

        if let Some(previous_mark) = self.previous_commit_mark {
            writeln!(self.output, "from :{previous_mark}")?;
        }

        self.previous_commit_mark = Some(self.current_mark);

        for (mark, path) in paths_to_nodes {
            if path.is_empty() {
                continue;
            }
            writeln!(self.output, "M 100644 :{mark} {path}")?;
        }
        writeln!(self.output)?;
        self.commit_count += 1;
        if self.commit_count % 10 == 0 {
            writeln!(
                self.output,
                "progress [{}] Commit: {}/{}",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                self.commit_count,
                self.total
            )?;
        }
        Ok(())
    }

    pub fn add_file(&mut self, data: Vec<u8>) -> io::Result<usize> {
        self.current_mark += 1;
        writeln!(self.output, "blob")?;
        writeln!(self.output, "mark :{}", self.current_mark)?;
        if self.skip_contents {
            writeln!(self.output, "data 1")?;
            self.output.write_all(b"d")?;
        } else {
            writeln!(self.output, "data {}", data.len())?;
            self.output.write_all(&data)?;
        }

        writeln!(self.output)?;
        Ok(self.current_mark)
    }
}
