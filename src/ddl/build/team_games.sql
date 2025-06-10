create table if not exists build.team_games
(
    game_id string
    , team_id string
    , box_score int[]
    , extra_innings int
    , manager_id string
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
    , win int
    , loss int
    , tie int
    , runs_scored int
    , runs_allowed int
    , yr int
    , mo int
    , dy int
    , game_number int
    , gametype string
    , primary key (game_id, team_id)
)
;