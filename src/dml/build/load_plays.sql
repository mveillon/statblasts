copy (
    select gid || pn || uuid() as play_id
    , gid as game_id
    , inning
    , top_bot as is_bottom_inning
    , vis_home as is_visiting_batting
    , batter
    , pitcher
    , outs_pre
    , count
    , nump as num_pitches
    , pa as was_pa
    , ab as was_ab
    , case
        when outs_post > outs_pre
        then 1
        else 0
    end as was_out
    , case
        when single = 1
        then 'single'
        when double = 1
        then 'double'
        when triple = 1
        then 'triple'
        when hr = 1
        then 'home_run'
        when sh = 1
        then 'sac_bunt'
        when sf = 1
        then 'sac_fly'
        when hbp = 1
        then 'hit_by_pitch'
        when walk = 1
        then 'walk'
        when iw = 1
        then 'int_walk'
        when k = 1
        then 'strikeout'
        when xi = 1
        then 'catchers_interference'
        when (oth + othout + noout + oa) >= 1
        then 'other'
        when (gdp + othdp) >= 1
        then 'double_play'
        when tp = 1
        then 'triple_play'
        when wp = 1
        then 'wild_pitch'
        when pb = 1
        then 'passed_ball'
        when bk = 1
        then 'balk'
        when di = 1
        then 'defensive_indifference'
        when (sb2 + sb3 + sbh) = 1
        then 'stolen_base'
        when (cs2 + cs3 + csh + pko1 + pko2 + pko3) = 1
        then 'caught_stealing'
        when k_safe = 1
        then 'dropped_third_strike_safe'
        when (e1 + e2 + e3 + e4 + e5 + e6 + e7 + e8 + e9) >= 1
        then 'error'
        else null
    end as event_type
    , case
        when bunt = 1
        then 'bunt'
        when ground = 1
        then 'ground_ball'
        when fly = 1
        then 'fly_ball'
        when "line" = 1
        then 'line_drive'
        else null
    end as hit_trajectory
    , case
        when pko1 = 1
        then 1
        when (sb2 + cs2 + pko2) >= 1
        then 2
        when (sb3 + cs3 + pko3) >= 1
        then 3
        when (sbh + csh) >= 1
        then 4
        else null
    end as base_attempted_steal_at
    , list_filter(
        [
            case when e1 = 1 then 1 else 0 end,
            case when e2 = 1 then 2 else 0 end,
            case when e3 = 1 then 3 else 0 end,
            case when e4 = 1 then 4 else 0 end,
            case when e5 = 1 then 5 else 0 end,
            case when e6 = 1 then 6 else 0 end,
            case when e7 = 1 then 7 else 0 end,
            case when e8 = 1 then 8 else 0 end,
            case when e9 = 1 then 9 else 0 end,
        ],
        p -> p > 0
    ) as error_positions
    , nullif(br1_pre, '') as on_first_pre
    , nullif(br2_pre, '') as on_second_pre
    , nullif(br3_pre, '') as on_third_pre
    , nullif(br1_post, '') as on_first_post
    , nullif(br2_post, '') as on_second_post
    , nullif(br3_post, '') as on_third_post
    , case when run_b = '' then null else pitcher end as pitcher_charged_batter
    , nullif(prun1, '') as pitcher_charged_first
    , nullif(prun2, '') as pitcher_charged_second
    , nullif(prun3, '') as pitcher_charged_third
    , rbi
    , er as earned_runs
    , tur as unearned_runs
    , case when firstf = 0 then null else firstf end as fielder
    , "date" // 10000 as yr
    , ("date" - (yr * 10000)) // 100 as mo
    , "date" % 100 as dy
    from 'data/raw/plays.csv'
    where "date" between {{ start }} * 10000 and {{ end }} * 10000
    and coalesce(event_type, 'other') != 'other'
) to 'data/build/plays'
(
    format parquet,
    partition_by (yr, mo, dy),
    overwrite true
)
;