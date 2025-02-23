copy (
    select gid as game_id
    , team as team_id
    , [
        case when regexp_full_match(inn1, '[0-9]') then cast(inn1 as int) else null end,
        case when regexp_full_match(inn1, '[0-9]') then cast(inn1 as int) else null end,
        case when regexp_full_match(inn1, '[0-9]') then cast(inn1 as int) else null end,
        case when regexp_full_match(inn1, '[0-9]') then cast(inn1 as int) else null end,
        case when regexp_full_match(inn1, '[0-9]') then cast(inn1 as int) else null end,
        case when regexp_full_match(inn1, '[0-9]') then cast(inn1 as int) else null end,
        case when regexp_full_match(inn1, '[0-9]') then cast(inn1 as int) else null end,
        case when regexp_full_match(inn1, '[0-9]') then cast(inn1 as int) else null end,
        case when regexp_full_match(inn1, '[0-9]') then cast(inn1 as int) else null end,
        -- pain
        (
            case when regexp_full_match(inn10, '[0-9]') then cast(inn10 as int) else 0 end
            + case when regexp_full_match(inn11, '[0-9]') then cast(inn11 as int) else 0 end
            + case when regexp_full_match(inn12, '[0-9]') then cast(inn12 as int) else 0 end
            + case when regexp_full_match(inn13, '[0-9]') then cast(inn13 as int) else 0 end
            + case when regexp_full_match(inn14, '[0-9]') then cast(inn14 as int) else 0 end
            + case when regexp_full_match(inn15, '[0-9]') then cast(inn15 as int) else 0 end
            + case when regexp_full_match(inn16, '[0-9]') then cast(inn16 as int) else 0 end
            + case when regexp_full_match(inn17, '[0-9]') then cast(inn17 as int) else 0 end
            + case when regexp_full_match(inn18, '[0-9]') then cast(inn18 as int) else 0 end
            + case when regexp_full_match(inn19, '[0-9]') then cast(inn19 as int) else 0 end
            + case when regexp_full_match(inn20, '[0-9]') then cast(inn20 as int) else 0 end
            + case when regexp_full_match(inn21, '[0-9]') then cast(inn21 as int) else 0 end
            + case when regexp_full_match(inn22, '[0-9]') then cast(inn22 as int) else 0 end
            + case when regexp_full_match(inn23, '[0-9]') then cast(inn23 as int) else 0 end
            + case when regexp_full_match(inn24, '[0-9]') then cast(inn24 as int) else 0 end
            + case when regexp_full_match(inn25, '[0-9]') then cast(inn25 as int) else 0 end
            + case when regexp_full_match(inn26, '[0-9]') then cast(inn26 as int) else 0 end
            + case when regexp_full_match(inn27, '[0-9]') then cast(inn27 as int) else 0 end
            + case when regexp_full_match(inn28, '[0-9]') then cast(inn28 as int) else 0 end
        )
    ] as box_score
    , case when regexp_full_match(inn10, '[0-9]') then 1 else 0 end as extra_innings
    , mgr as manager_id
    , b_pa as plate_appearances
    , b_ab as at_bats
    , (b_h - b_d - b_t - b_hr) as singles
    , b_d as doubles
    , b_t as triples
    , b_hr as home_runs
    , b_rbi as rbi
    , b_sh as sacrifice_hits
    , b_sf as sacrifice_flies
    , b_hbp as hit_by_pitches
    , (cast(nullif(b_w, '?') as int)) as walks
    , b_iw as int_walks
    , (cast(nullif(b_k, '?') as int)) as strikeouts
    , b_sb as stolen_bases
    , b_cs as times_caught_stealing
    , b_gdp as times_ground_double_play
    , b_xi as times_catchers_interference
    , b_roe as times_reached_on_error
    , p_bfp as batters_faced
    , (p_h - p_d - p_t - p_hr) as singles_allowed
    , p_d as doubles_allowed
    , p_t as triples_allowed
    , p_hr as home_runs_allowed
    , p_er as earned_runs_allowed
    , (p_r - p_er) as unearned_runs_allowed
    , (cast(nullif(p_w, '?') as int)) as walks_allowed
    , p_iw as int_walks_allowed
    , (cast(nullif(p_k, '?') as int)) as pitcher_strikeouts
    , p_hbp as hit_by_pitches_allowed
    , p_wp as wild_pitches
    , p_pb as passed_balls
    , p_bk as balks_allowed
    , p_sh as sacrifice_hits_allowed
    , p_sf as sacrifice_flies_allowed
    , p_sb as stolen_bases_allowed
    , p_cs as pitcher_caught_stealing
    , d_po as putouts
    , d_a as assists
    , d_e as errors
    , d_dp as pitcher_double_plays
    , d_tp as pitcher_triple_plays
    , win
    , loss
    , tie
    , "date" // 10000 as yr
    from read_csv(
        'data/raw/teamstats.csv',
        header = true,
        types = {
            'inn1': 'varchar',
            'inn2': 'varchar',
            'inn3': 'varchar',
            'inn4': 'varchar',
            'inn5': 'varchar',
            'inn6': 'varchar',
            'inn7': 'varchar',
            'inn8': 'varchar',
            'inn9': 'varchar',
            'inn10': 'varchar',
            'inn11': 'varchar',
            'inn12': 'varchar',
            'inn13': 'varchar',
            'inn14': 'varchar',
            'inn15': 'varchar',
            'inn16': 'varchar',
            'inn17': 'varchar',
            'inn18': 'varchar',
            'inn19': 'varchar',
            'inn20': 'varchar',
            'inn21': 'varchar',
            'inn22': 'varchar',
            'inn23': 'varchar',
            'inn24': 'varchar',
            'inn25': 'varchar',
            'inn26': 'varchar',
            'inn27': 'varchar',
            'inn28': 'varchar',
            'b_k': 'varchar',
            'p_k': 'varchar',
            'b_w': 'varchar',
            'p_w': 'varchar'
        }
    )
    where stattype = 'value'
    and gametype = 'regular'
)
to 'data/build/team_games'
(
    format parquet,
    partition_by (yr, team_id),
    overwrite true
)
;