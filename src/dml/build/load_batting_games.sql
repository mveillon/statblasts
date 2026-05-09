delete from build.batting_games
where yr between {{ start }} and {{ end }}
;

insert into build.batting_games
by name
(
    with partitioned as (
        select gid as game_id
        , id as player_id
        , b_lp as lineup_pos
        , case
            when b_seq = '1'
            then 0
            else 1
        end as was_pinch_hitter
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
        , case when vishome = 'v' then 0 when vishome = 'h' then 1 end as is_home
        , "date" // 10000 as yr
        , ("date" // 100) % 100 as mo
        , "date" % 100 as dy
        , gametype
        , row_number() over (partition by game_id, player_id order by case when gametype = 'regular' then 1 end desc) as rn
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
                'number': 'int'
            }
        )
        where 1=1
        and yr between {{ start }} and {{ end }}
        and stattype = 'value'
    )
    select game_id
    , player_id
    , lineup_pos
    , was_pinch_hitter
    , was_pinch_runner
    , plate_appearances
    , at_bats
    , runs
    , singles
    , doubles
    , triples
    , home_runs
    , rbi
    , sacrifice_hits
    , sacrifice_flies
    , hit_by_pitches
    , walks
    , intentional_walks
    , strikeouts
    , stolen_bases
    , times_caught_stealing
    , times_ground_double_play
    , times_catchers_interference
    , times_reached_on_error
    , is_home
    , yr
    , mo
    , dy
    , gametype
    from partitioned
    where rn = 1
)
;