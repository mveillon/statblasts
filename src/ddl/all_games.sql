copy (
    select gid as game_id
    , ballpark
    , "date" // 10000 as yr
    , ("date" - (yr * 10000)) // 100 as mo
    , "date" % 100 as dy
    from 'data/raw/plays.csv'
    where gametype = 'regular'
    and pbp = 'full'
    and "date" >= 2020 * 10000
)
to 'data/build/all_games'
(
    format parquet,
    partition_by (yr, mo, dy),
    overwrite true
)
;
