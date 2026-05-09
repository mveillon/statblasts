create table if not exists build.batting_games
(
    game_id string
    , player_id string
    , lineup_pos int
    , was_pinch_hitter int
    , was_pinch_runner int
    , plate_appearances int
    , at_bats int
    , runs int
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
    , is_home int
    , yr int
    , mo int
    , dy int
    , gametype string
    , primary key (game_id, player_id)
)
;