create table if not exists build.players
(
    player_id string
    , first_name string
    , last_name string
    , bat_hand string
    , throw_hand string
    , primary_position string
    , debut_year int
    , final_year int
    , primary key (player_id)
)
;