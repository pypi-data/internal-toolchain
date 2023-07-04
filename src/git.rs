use chrono::prelude::*;
use std::io;
use std::io::Write;
use std::sync::Mutex;

#[derive(Debug)]
pub struct GitFastImporter<T: Write> {
    output: T,
    current_mark: usize,
    previous_commit_mark: Option<usize>,
    has_code_branch: bool,
    has_python_code_branch: bool,
    total: usize,
    commit_count: usize,
}

#[derive(Copy, Clone, Debug)]
pub enum Branch {
    Code,
    PythonCode,
}

impl From<Branch> for &'static str {
    fn from(val: Branch) -> Self {
        match val {
            Branch::Code => "all",
            Branch::PythonCode => "python",
        }
    }
}

impl<T: Write> GitFastImporter<T> {
    pub fn new(
        output: T,
        total: usize,
        has_code_branch: bool,
        has_python_code_branch: bool,
    ) -> Mutex<Self> {
        Mutex::new(GitFastImporter {
            output,
            current_mark: 0,
            previous_commit_mark: None,
            has_code_branch,
            has_python_code_branch,
            commit_count: 0,
            total,
        })
    }

    pub fn finish(&mut self) -> io::Result<()> {
        writeln!(self.output, "done")?;
        Ok(())
    }

    pub fn flush_commit(
        &mut self,
        name: &str,
        branch: Branch,
        paths_to_nodes: Vec<(usize, String)>,
        prefix: Option<String>,
    ) -> io::Result<()> {
        self.current_mark += 1;
        let now = Utc::now();
        let branch_name: &'static str = branch.into();
        writeln!(self.output, "commit refs/heads/{}", branch_name)?;
        writeln!(self.output, "mark :{}", self.current_mark)?;
        writeln!(
            self.output,
            "committer Bot <41898282+github-actions[bot]@users.noreply.github.com> {} +0000",
            now.timestamp()
        )?;

        let commit_message = format!("Add package {name}");
        writeln!(self.output, "data {}", commit_message.len())?;
        writeln!(self.output, "{commit_message}")?;

        let (should_use_from, update_bool) = match branch {
            Branch::Code => (self.has_code_branch, &mut self.has_code_branch),
            Branch::PythonCode => (
                self.has_python_code_branch,
                &mut self.has_python_code_branch,
            ),
        };

        if should_use_from {
            writeln!(self.output, "from {}", branch_name)?;
            *update_bool = true;
        }

        if let Some(previous_mark) = self.previous_commit_mark {
            writeln!(self.output, "from :{previous_mark}")?;
        }

        self.previous_commit_mark = Some(self.current_mark);

        for (mark, path) in paths_to_nodes {
            let path = if path.contains("./") {
                path.replace("./", "")
            } else {
                path
            };
            if path.is_empty() {
                continue;
            }
            write!(self.output, "M 100644 :{mark} ")?;
            if let Some(prefix) = &prefix {
                write!(self.output, "{prefix}")?;
            }
            writeln!(self.output, "{path}")?;
        }
        writeln!(self.output)?;
        Ok(())
    }

    pub fn flush_progress(&mut self) -> io::Result<()> {
        self.commit_count += 1;
        if self.commit_count % 50 == 0 {
            writeln!(
                self.output,
                "progress Commit: {}/{}",
                self.commit_count, self.total
            )?;
        }
        Ok(())
    }

    pub fn add_file(&mut self, data: Vec<u8>) -> io::Result<usize> {
        self.current_mark += 1;
        writeln!(self.output, "blob")?;
        writeln!(self.output, "mark :{}", self.current_mark)?;
        writeln!(self.output, "data {}", data.len())?;
        self.output.write_all(&data)?;
        writeln!(self.output)?;
        Ok(self.current_mark)
    }
}
