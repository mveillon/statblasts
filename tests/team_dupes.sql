with teams as (
    select *
    from 'data/build/teams.csv'
),
dupes as (
    select team_id
    from teams
    group by 1
    having count(*) > 1
)
select *
from teams t
where exists (
    select 1
    from dupes
    where team_id = t.team_id
)
;