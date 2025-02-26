copy (
    select gid as game_id
    , id as player_id
    , b_lp as lineup_pos
    , case
        when b_seq = '1'
        then 0
        else 1
    end as is_pinch_hitter
    , pr as was_pinch_runner
    , b_pa as plate_appearances
    , b_ab as at_bats
    , b_r as runs
    , (b_h - b_d - b_t - b_hr) as singles
    , b_d as doubles
    , b_t as triples
    , b_hr as home_runs
    , b_rbi as rbi
    , b_sh as sacrifice_hits
    , b_sf as sacrifice_flies
    , b_hbp as hit_by_pitches
    , b_w as walks
    , b_iw as intentional_walks
    , b_k as strikeouts
    , b_sb as stolen_bases
    , b_cs as times_caught_stealing
    , b_gdp as times_ground_double_play
    , b_xi as times_catchers_interference
    , b_roe as times_reached_on_error
    , "date" // 10000 as yr
    , ("date" // 100) % 100 as mo
    , "date" % 100 as dy
    from read_csv(
        'data/raw/batting.csv',
        header = true,
        types = {
            'b_seq': 'varchar',
            'b_sf': 'int',
            'b_iw': 'int',
            'b_cs': 'int',
            'b_gdp': 'int',
            'b_roe': 'int',
            'dh': 'int',
            'number': 'int',
            'vishome': 'int'
        }
    )
    where "date" between {{ start }} * 10000 and {{ end }} * 10000
    and stattype = 'value'
)
to 'data/build/batting_games'
(
    format parquet,
    partition_by (yr, mo, dy),
    overwrite true
)
;