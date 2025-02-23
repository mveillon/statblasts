copy (
    select team as team_id
    , city as location
    , nickname
    , make_date(
        first_g // 10000
        , (first_g // 100) % 100
        , first_g % 100
    ) as first_game
    , make_date(
        last_g // 10000
        , (last_g // 100) % 100
        , last_g % 100
    ) as last_game
    from 'data/raw/biodata/teams0.csv'
)
to 'data/build/teams.csv'
(
    format csv,
    overwrite true
)
;