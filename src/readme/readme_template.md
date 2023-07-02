# {name}

This repo contains {total_packages} packages uploaded to PyPi between 
{first_package_time} and {last_package_time}.

The repo is {percent_done}% ({done_count}/{total_packages}) complete.

## Packages:

| Name  | Count |
| ----- | ----- |
{{ for x in table_data -}}
| {x.0} | {x.1} |
{{ endfor }}
