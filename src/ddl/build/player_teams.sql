create table if not exists build.player_teams
(
    player_id string
    , team_abbr string
    , yr int
    , primary key (player_id, team_abbr, yr)
)
;