copy (
    select gid as game_id
    , id as player_id
    , case
        when p_seq = '1'
        then 1
        else 0
    end as is_starter
    , p_ipouts as outs_recorded
    , p_bfp as batters_faced
    , (p_h - p_d - p_t - p_hr) as singles
    , p_d as doubles
    , p_t as triples
    , p_hr as home_runs
    , p_er as earned_runs
    , (p_r - p_er) as unearned_runs
    , p_w as walks
    , p_iw as intentional_walks
    , p_k as strikeouts
    , p_hbp as hit_by_pitches
    , p_wp as wild_pitches
    , p_pb as passed_balls
    , p_bk as balks
    , p_sh as sacrifice_hits
    , p_sf as sacrifice_flies
    , p_sb as stolen_bases
    , p_cs as caught_stealing
    , case
        when wp = 1
        then 'win'
        when lp = 1
        then 'loss'
        when save = 1
        then 'save'
    end as award
    , p_cg as complete_game
    , "date" // 10000 as yr
    , ("date" // 100) % 100 as mo
    , "date" % 100 as dy
    from read_csv(
        'data/raw/pitching.csv',
        header = true,
        types = {
            'p_seq': 'varchar'
        }
    )
    where stattype = 'value'
    and gametype = 'regular'
    and yr between {{ start }} and {{ end }}
)
to 'data/build/pitching_games'
(
    format parquet,
    partition_by (yr, mo, dy),
    overwrite true
)
;