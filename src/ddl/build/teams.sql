create table if not exists build.teams
(
    team_id string
    , location string
    , nickname string
    , first_game date
    , last_game date
    , primary key (team_id)
)
;