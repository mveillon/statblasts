delete from build.fielding_games
where yr between {{ start }} and {{ end }}
;

insert into build.fielding_games
by name
(
    with partitioned as (
        select gid as game_id
        , id as player_id
        , try_cast(d_pos as int) as position_id
        , d_po as putouts
        , d_a as assists
        , d_e as errors
        , d_dp as double_plays
        , d_tp as triple_plays
        , case when vishome = 'v' then 0 when vishome = 'h' then 1 end as is_home
        , "date" // 10000 as yr
        , ("date" // 100) % 100 as mo
        , "date" % 100 as dy
        , gametype
        , row_number() over (partition by game_id, player_id order by case when gametype = 'regular' then 1 end desc) as rn
        from read_csv(
            'data/raw/fielding.csv',
            header = true,
            types = {
                'd_pos': 'varchar',
                'd_sb': 'int',
                'd_cs': 'int',
                'number': 'int'
            }
        )
        where stattype = 'value'
        and yr between {{ start }} and {{ end }}
    )
    select game_id
    , player_id
    , position_id
    , putouts
    , assists
    , errors
    , double_plays
    , triple_plays
    , is_home
    , yr
    , mo
    , dy
    , gametype
    from partitioned
    where rn = 1
)
;