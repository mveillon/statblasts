with events as (
    select *
    from 'data/build/events/*/*/*/*.parquet'
),
dupes as (
    select play_id
    from events
    group by 1
    having count(*) > 1
)
select *
from events e
where exists (
    select 1
    from dupes
    where play_id = e.play_id
)
limit 10
;