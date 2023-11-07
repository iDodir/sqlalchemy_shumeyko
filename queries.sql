select *
from resume;

select workload, avg(compensation)::int as avg_compensation
from resume
where title like '%Python%'
  and compensation > 40000
group by workload;

select w.id,
       w.username,
       r.compensation,
       r.workload,
       avg(r.compensation) over (partition by workload)::int as avg_workload_compensation,
       compensation - avg(r.compensation) over (partition by workload)::int
from resume r
         join worker w
              on w.id = r.worker_id;

select *,
       compensation - avg_workload_compensation as compensation_diff
from (select w.id,
             w.username,
             r.compensation,
             r.workload,
             avg(r.compensation) over (partition by workload)::int as avg_workload_compensation
      from resume r
               join worker w
                    on w.id = r.worker_id) helper1;

with helper2 as (select *,
                        compensation - avg_workload_compensation as compensation_diff
                 from (select w.id,
                              w.username,
                              r.compensation,
                              r.workload,
                              avg(r.compensation) over (partition by workload)::int as avg_workload_compensation
                       from resume r
                                join worker w
                                     on w.id = r.worker_id) helper1)
select *
from helper2
order by compensation_diff desc;
