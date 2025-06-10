copy (
    with totals as (
        select player_id
        , yr
        , gametype
        , array_distinct(array_agg(position_id)) as positions
        , sum(putouts) as putouts
        , sum(assists) as assists
        , sum(errors) as errors
        , sum(double_plays) as double_plays
        , sum(triple_plays) as triple_plays
        from 'data/build/fielding_games/*/*/*.parquet'
        where yr between {{ start }} and {{ end }}
        group by yr, player_id, gametype
    ),
    averages as (
        select player_id
        , yr
        , gametype
        , positions
        , putouts
        , assists
        , errors
        , double_plays
        , triple_plays
        , (putouts + assists) / (putouts + assists + errors) as fielding_percentage
        from totals
    )
    select player_id
    , yr
    , gametype
    , positions
    , putouts
    , assists
    , errors
    , double_plays
    , triple_plays
    , nullif(fielding_percentage, 'nan') as fielding_percentage
    from averages
)
to 'data/publish/fielding_yearly'
(
    format parquet,
    partition_by (yr, gametype),
    overwrite true
)
;