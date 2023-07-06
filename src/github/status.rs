use crate::github;
use crate::github::GithubError;
use rand::seq::IteratorRandom;
use rand::thread_rng;



pub fn get_status(github_token: &str, sample: Option<usize>, progress_less_than: usize) -> Result<(), GithubError> {
    let all_repos = github::projects::get_all_pypi_data_repos(&github_token)?;
    let client = github::get_client();
    let indexes: Result<Vec<(_, _)>, _> = all_repos
        .iter()
        .map(|name| {
            github::index::get_repository_index(&github_token, name, Some(client.clone()))
                .map(|r| (name, r))
        })
        .collect();
    let mut indexes = indexes?;
    // let runs: Result<Vec<_>, _> = all_repos
    //     .iter()
    //     .map(|name| {
    //         github::workflows::get_workflow_runs(&github_token, name, Some(client.clone()))
    //     })
    //     .collect();
    // let runs = runs?;

    if let Some(sample) = sample {
        let mut rng = thread_rng();
        indexes = indexes.into_iter().choose_multiple(&mut rng, sample);
    }

    for (name, index) in indexes {
        let stats = index.stats();
        if stats.percent_done() < progress_less_than {
            println!("{name}")
        }
        eprintln!("Stats: {stats:?}: percent done: {}%", stats.percent_done());
    }
    Ok(())
}
