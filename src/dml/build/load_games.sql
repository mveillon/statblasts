copy (
    with games as (
        select gid as game_id
        , ballpark
        , "date" // 10000 as yr
        , ("date" // 100) % 100 as mo
        , "date" % 100 as dy
        , case
            when vis_home = 0
            then batteam
            else pitteam
        end as visting_team
        , case
            when vis_home = 0
            then pitteam
            else batteam
        end as home_team
        , row_number() over (partition by gid order by "date") = 1 as rn
        from 'data/raw/plays.csv'
        where gametype = 'regular'
        and pbp = 'full'
        and "date" between {{ start }} * 10000 and {{ end }} * 10000
    )
    select game_id
    , ballpark
    , yr
    , mo
    , dy
    , visting_team
    , home_team
    from games
    where rn = 1
)
to 'data/build/games'
(
    format parquet,
    partition_by (yr, mo, dy),
    overwrite true
)
;
