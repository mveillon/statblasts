create table if not exists build.lineups
(
    game_id string
    , team_id string
    , yr int
    , lineup string[]
    , pitcher string
    , catcher string
    , first_base string
    , second_base string
    , third_base string
    , shortstop string
    , left_field string
    , center_field string
    , right_field string
    , gametype string
    , primary key (game_id, team_id)
)
;