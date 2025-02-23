with players as (
    select *
    from 'data/build/players/*/*.parquet'
),
dupes as (
    select player_id
    from players
    group by 1
    having count(*) > 1
)
select *
from players p
where exists (
    select 1
    from dupes
    where player_id = p.player_id
)
;