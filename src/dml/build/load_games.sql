copy (
    select gid as game_id
    , visteam as visting_team
    , hometeam as home_team
    , "date" // 10000 as yr
    , ("date" // 100) % 100 as mo
    , "date" % 100 as dy
    , case
        when daynight = 'day'
        then 1
        else 0
    end as is_day_game
    , usedh as used_dh
    , timeofgame as time_of_game
    , try_cast(attendance as int) as attendance
    , try_cast(temp as int) as game_temp
    , winddir as wind_direction
    , nullif(try_cast(windspeed as int), -1) as wind_speed
    , wp as winning_pitcher
    , lp as losing_pitcher
    , save as saving_pitcher
    , vruns as visting_score
    , hruns as home_score
    from read_csv(
        'data/raw/gameinfo.csv',
        header = true,
        types = {
            'attendance': 'varchar',
            'temp': 'varchar',
            'windspeed': 'varchar',
            'innings': 'int',
            'tiebreaker': 'int',
            'forfeit': 'int',
            'suspend': 'int'
        }
    )
    where gametype = 'regular'
    and yr between {{ start }} and {{ end }}
)
to 'data/build/games'
(
    format parquet,
    partition_by (yr, mo, dy),
    overwrite true
)
;
