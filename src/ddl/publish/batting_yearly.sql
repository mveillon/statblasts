create table if not exists publish.batting_yearly
(
    player_id string
    , yr int
    , gametype string
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
    , batting_average float
    , on_base_percentage float
    , slugging_percentage float
    , strikeout_percentage float
    , walk_percentage float
    , isolated_power float
    , ops float
    , primary key (player_id, yr, gametype)
)
;