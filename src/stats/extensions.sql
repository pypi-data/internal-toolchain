select extension,
       count() as total_count,
       sum(size) / 1024 / 1024 / 1024 as total_size_gb,
       min(size) as min_size,
       CEIL(avg(size)) / 1024 as avg_size_mb,
       max(size) / 1024 / 1024 / 1024 as max_size_gb,
       sum(lines) as total_lines,
       min(lines) as min_lines,
       CEIL(avg(lines)) as avg_lines,
       max(lines) as max_lines
from data
where content_type = 'too-large' and size != 0
group by extension
order by total_size_gb desc
limit 15