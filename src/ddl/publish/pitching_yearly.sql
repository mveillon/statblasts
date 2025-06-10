create table if not exists publish.pitching_yearly
(
    player_id string
    , yr int
    , starts int
    , outs_recorded int
    , batters_faced int
    , singles int
    , doubles int
    , triples int
    , home_runs int
    , earned_runs int
    , unearned_runs int
    , walks int
    , intentional_walks int
    , strikeouts int
    , hit_by_pitches int
    , wild_pitches int
    , passed_balls int
    , balks int
    , sacrifice_hits int
    , sacrifice_flies int
    , stolen_bases int
    , caught_stealing int
    , wins int
    , losses int
    , saves int
    , complete_games int
    , earned_run_average float
    , strikeout_percentage float
    , walk_percentage float
    , innings_pitched string
    , primary key (player_id, yr)
)
;