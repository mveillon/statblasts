with average_runs as (
    select team_id
    , yr
    , avg(runs_scored) / 9 as avg_runs_per_inning
    from publish.teams_yearly
    where yr between {{ start }} and {{ end }}
    and gametype = 'regular'
    group by 1, 2
),
pitcher_games as (
    select p.player_id
    , p.yr
    , p.batters_faced
    , p.earned_runs as runs_allowed
    , case when p.award = 'win' then 1 else 0 end as is_win
    , case
        when p.is_home = 0
        then g.visiting_team
        when p.is_home = 1
        then g.home_team
    end as team_id
    from build.pitching_games p
    join build.games g
        on p.game_id = g.game_id
        and p.yr = g.yr
    where p.yr between {{ start }} and {{ end }}
    and p.gametype = 'regular'
    and g.yr between {{ start }} and {{ end }}
    and g.gametype = 'regular'
    and is_starter = 1
),
expected_wins as (
    select p.player_id
    , p.yr
    , sum(is_win) as wins
    , sum(
        case
            when p.runs_allowed < a.avg_runs_per_inning * p.batters_faced / 3
            then 1
            else 0
        end
    ) as expected_wins
    from pitcher_games p
    join average_runs a
        on p.team_id = a.team_id
        and p.yr = a.yr
    group by p.player_id, p.yr
),
biggest_diff as (
    select player_id
    , sum(wins) as career_wins
    , sum(expected_wins) as expected_wins
    , sum(expected_wins - wins) as stolen_wins
    from expected_wins
    group by 1
)
select e.player_id
, e.career_wins
, e.expected_wins
, e.stolen_wins
, p.first_name
, p.last_name
, p.debut_year
, p.final_year
from biggest_diff e
join build.players p
    on e.player_id = p.player_id
order by stolen_wins asc
limit 10
;


