copy (
    with players as (
        select id as player_id
        , first as first_name
        , last as last_name
        , case
            when bat = 'B'
            then 'S'
            when bat not in ('L', 'R')
            then null
            else bat
        end as bat_hand
        , case
            when throw = 'B'
            then 'S'
            when throw not in ('L', 'R')
            then null
            else throw
        end as throw_hand
        , list_max([g_p, g_rp, g_c, g_1b, g_2b, g_3b, g_ss, g_lf, g_cf, g_rf, g_dh]) as main_position_count
        , g_p as games_pitcher
        , g_rp as games_relief_pitcher
        , g_c as games_catcher
        , g_1b as games_first_base
        , g_2b as games_second_base
        , g_3b as games_third_base
        , g_ss as games_shortstop
        , g_lf as games_left_field
        , g_cf as games_center_field
        , g_rf as games_right_field
        , g_dh as games_designated_hitter
        , g_pr as games_pinch_runner
        , g_ph as games_pinch_hitter
        , season
        from 'data/raw/allplayers.csv'
        where season between {{ start }} and {{ end }}
    ),
    partitioned as (
        select player_id
        , first_name
        , last_name
        , mode(bat_hand) over (partition by player_id) as bat_hand
        , mode(throw_hand) over (partition by player_id) as throw_hand
        , case
            when games_pitcher >= main_position_count
            then 'P'
            when games_catcher >= main_position_count
            then 'C'
            when games_first_base >= main_position_count
            then '1B'
            when games_second_base >= main_position_count
            then '2B'
            when games_third_base >= main_position_count
            then '3B'
            when games_shortstop >= main_position_count
            then 'SS'
            when games_left_field >= main_position_count
            then 'LF'
            when games_center_field >= main_position_count
            then 'CF'
            when games_right_field >= main_position_count
            then 'RF'
            when games_designated_hitter >= main_position_count
            then 'DH'
            else null
        end as primary_position
        , row_number() over (
            partition by player_id
            order by season desc
        ) as rn
        from players
    )
    select player_id
    , first_name
    , last_name
    , bat_hand
    , throw_hand
    , primary_position
    from partitioned
    where rn = 1
)
to 'data/build/players'
(
    format parquet,
    partition_by (primary_position),
    overwrite true
)