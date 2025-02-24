copy (
    select gid as game_id
    , team as team_id
    , "date" // 10000 as yr
    , [
        start_l1,
        start_l2,
        start_l3,
        start_l4,
        start_l5,
        start_l6,
        start_l7,
        start_l8,
        start_l9
    ] as lineup
    , start_f1 as pitcher
    , start_f2 as catcher
    , start_f3 as first_base
    , start_f4 as second_base
    , start_f5 as third_base
    , start_f6 as shortstop
    , start_f7 as left_field
    , start_f8 as center_field
    , start_f9 as right_field
    from 'data/raw/teamstats.csv'
    where stattype = 'value'
    and gametype = 'regular'
    and yr between {{ start }} and {{ end }}
)
to 'data/build/lineups'
(
    format parquet,
    partition_by (yr, team_id),
    overwrite true
)
;