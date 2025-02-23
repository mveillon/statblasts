copy (
    with rosters as (
        select player_id
        , team_abbr
        , parse_filename(filename, true) as filename
        from read_csv(
            'data/raw/files_raw/rosters/*.csv', -- originally the *.ROS files
            names = [
                player_id
                , last_name
                , first_name
                , bat_hand
                , throw_hand
                , team_abbr
                , position
            ],
            header = false,
            filename = true
        )
    )
    select distinct player_id
    , team_abbr
    , cast(regexp_replace(filename, '[^0-9]', '', 'g') as int) as yr
    from rosters
)
to 'data/build/player_teams'
(
    format parquet,
    partition_by (yr),
    overwrite true
)