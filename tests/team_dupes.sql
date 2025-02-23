with teams as (
    select *
    from 'data/build/teams/*/*.parquet'
),
dupes as (
    select team_abbr
    , yr
    from teams
    group by 1, 2
    having count(*) > 1
)
select *
from teams t
where exists (
    select 1
    from dupes
    where team_abbr = t.team_abbr
    and yr = t.yr
)
;