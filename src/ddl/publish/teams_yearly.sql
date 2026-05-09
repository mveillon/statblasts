create table if not exists publish.teams_yearly
(
    team_id string
    , yr int
    , gametype string
    , wins int
    , losses int
    , ties int
    , plate_appearances int
    , at_bats int
    , singles int
    , doubles int
    , triples int
    , home_runs int
    , rbi int
    , sacrifice_hits int
    , sacrifice_flies int
    , hit_by_pitches int
    , walks int
    , intentional_walks int
    , strikeouts int
    , stolen_bases int
    , times_caught_stealing int
    , times_ground_double_play int
    , times_catchers_interference int
    , times_reached_on_error int
    , batters_faced int
    , singles_allowed int
    , doubles_allowed int
    , triples_allowed int
    , home_runs_allowed int
    , earned_runs_allowed int
    , unearned_runs_allowed int
    , walks_allowed int
    , intentional_walks_allowed int
    , pitcher_strikeouts int
    , hit_by_pitches_allowed int
    , wild_pitches int
    , passed_balls int
    , balks_allowed int
    , sacrifice_hits_allowed int
    , sacrifice_flies_allowed int
    , stolen_bases_allowed int
    , pitcher_caught_stealing int
    , putouts int
    , assists int
    , errors int
    , pitcher_double_plays int
    , pitcher_triple_plays int
    , runs_scored int
    , runs_allowed int
    , primary key (team_id, yr, gametype)
)
;