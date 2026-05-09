create table if not exists publish.fielding_yearly
(
    player_id string
    , yr int
    , gametype string
    , positions int[]
    , putouts int
    , assists int
    , errors int
    , double_plays int
    , triple_plays int
    , fielding_percentage float
    , primary key (player_id, yr, gametype)
)
;