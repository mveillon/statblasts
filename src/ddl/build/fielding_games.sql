create table if not exists build.fielding_games
(
    game_id string
    , player_id string
    , position_id int
    , putouts int
    , assists int
    , errors int
    , double_plays int
    , triple_plays int
    , yr int
    , mo int
    , dy int
    , gametype string
    , primary key (game_id, player_id)
)
;