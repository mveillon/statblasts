create table if not exists build.games
(
    game_id string
    , visiting_team string
    , home_team string
    , yr int
    , mo int
    , dy int
    , is_day_game int
    , used_dh int
    , time_of_game int
    , attendance int
    , game_temp int
    , wind_direction string
    , wind_speed int
    , winning_pitcher string
    , losing_pitcher string
    , saving_pitcher string
    , visting_score int
    , home_score int
    , gametype string
    , primary key (game_id)
)
;