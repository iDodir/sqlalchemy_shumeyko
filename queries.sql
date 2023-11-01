select * from resume;

select workload, avg(compensation)::int as avg_compensation
from resume
where title like '%Python%' and compensation > 40000
group by workload;
