copy (
    select gid as game_id
    , id as player_id
    , try_cast(d_pos as int) as position_id
    , d_po as putouts
    , d_a as assists
    , d_e as errors
    , d_dp as double_plays
    , d_tp as triple_plays
    , "date" // 10000 as yr
    , ("date" // 100) % 100 as mo
    , "date" % 100 as dy
    from read_csv(
        'data/raw/fielding.csv',
        header = true,
        types = {
            'd_pos': 'varchar'
        }
    )
    where stattype = 'value'
    and gametype = 'regular'
    and yr between {{ start }} and {{ end }}
)
to 'data/build/fielding_games'
(
    format parquet,
    partition_by (yr, mo, dy),
    overwrite true
)
;