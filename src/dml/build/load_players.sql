copy (
    with rosters as (
        select player_id
        , first_name
        , last_name
        , case when bat_hand = 'B' then 'S' else bat_hand end as bat_hand
        , case when throw_hand = 'B' then 'S' else throw_hand end as throw_hand
        , case
            when position.trim() = 'A'
            then 'P' -- for some reason only Brady Singer???
            else position.trim()
        end as position
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
    ),
    partitioned as (
        select player_id
        , first_name
        , last_name
        , mode(bat_hand) over (partition by player_id) as bat_hand
        , mode(throw_hand) over (partition by player_id) as throw_hand
        , mode(position) over (partition by player_id) as position
        , row_number() over (
            partition by player_id
            order by cast(regexp_replace(filename, '[^0-9]', '', 'g') as int) desc
        ) as rn
        from rosters
    )
    select player_id
    , first_name
    , last_name
    , bat_hand
    , throw_hand
    , position
    from partitioned
    where rn = 1
)
to 'data/build/players'
(
    format parquet,
    partition_by (position),
    overwrite true
)