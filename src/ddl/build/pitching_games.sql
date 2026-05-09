create table if not exists build.pitching_games
(
    game_id string
    , player_id string
    , is_starter int
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
    , award string
    , complete_game int
    , is_home int
    , gametype string
    , yr int
    , mo int
    , dy int
    , primary key (game_id, player_id)
)
;