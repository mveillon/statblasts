delete from build.fielding_games
where yr between {{ start }} and {{ end }}
;

insert into build.fielding_games
by name
(
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
    , gametype
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
;