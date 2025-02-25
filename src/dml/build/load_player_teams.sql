copy (
    select distinct id as player_id
    , team as team_abbr
    , season as yr
    from 'data/raw/allplayers.csv'
    where yr between {{ start }} and {{ end }}
)
to 'data/build/player_teams'
(
    format parquet,
    partition_by (yr),
    overwrite true
)