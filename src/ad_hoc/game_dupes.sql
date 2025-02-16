with games as (
    select *
    from 'data/build/games/*/*/*/*.parquet'
),
dupes as (
    select game_id
    from games
    group by 1
    having count(*) > 1
)
select *
from games g
where exists (
    select 1
    from dupes
    where game_id = g.game_id
)
;