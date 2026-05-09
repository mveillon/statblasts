delete from build.player_teams
where yr between {{ start }} and {{ end }}
;

insert into build.player_teams
by name
(
    select distinct id as player_id
    , team as team_abbr
    , season as yr
    from 'data/raw/allplayers.csv'
    where yr between {{ start }} and {{ end }}
)
;