with plays as (
    select *
    from 'data/build/plays/*/*/*/*.parquet'
),
dupes as (
    select play_id
    from plays
    group by 1
    having count(*) > 1
)
select *
from plays e
where exists (
    select 1
    from dupes
    where play_id = e.play_id
)
limit 10
;