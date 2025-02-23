with pts as (
    select *
    from 'data/build/player_teams/*/*.parquet'
),
dupes as (
    select player_id
    , team_abbr
    , yr
    from pts
    group by 1, 2, 3
    having count(*) > 1
)
select *
from pts p
where exists (
    select 1
    from dupes
    where player_id = p.player_id
    and team_abbr = p.team_abbr
    and yr = p.yr
)
;