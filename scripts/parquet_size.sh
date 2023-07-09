# Description: Get parquet size by column
parquet-layout "$1" \
  | jq '[.row_groups[].columns[] | . as $item | .pages[] | . += {"column": $item.path}] | group_by(.column)[] | {(.[0].column): {length: (. | length), compressed_mb: (([.[].compressed_bytes] | add) / 1024 / 1024), uncompressed_mb: (([.[].uncompressed_bytes] | add) / 1024 / 1024)}}' \
  | jq -s 'add'
