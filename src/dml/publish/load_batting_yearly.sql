copy (
    with totals as (
        select player_id
        , yr
        , sum(plate_appearances) as plate_appearances
        , sum(at_bats) as at_bats
        , sum(runs) as runs
        , sum(singles) as singles
        , sum(doubles) as doubles
        , sum(triples) as triples
        , sum(home_runs) as home_runs
        , sum(rbi) as rbi
        , sum(sacrifice_hits) as sacrifice_hits
        , sum(sacrifice_flies) as sacrifice_flies
        , sum(hit_by_pitches) as hit_by_pitches
        , sum(walks) as walks
        , sum(intentional_walks) as intentional_walks
        , sum(strikeouts) as strikeouts
        , sum(stolen_bases) as stolen_bases
        , sum(times_caught_stealing) as times_caught_stealing
        , sum(times_ground_double_play) as times_ground_double_play
        , sum(times_catchers_interference) as times_catchers_interference
        , sum(times_reached_on_error) as times_reached_on_error
        from 'data/build/batting_games/*/*/*/*.parquet'
        where yr between {{ start }} and {{ end }}
        group by yr, player_id
    ),
    averages as (
        select player_id
        , yr
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
        , (singles + doubles + triples + home_runs) / at_bats as batting_average
        , (singles + doubles + triples + home_runs + hit_by_pitches + walks + intentional_walks) / plate_appearances as on_base_percentage
        , (singles + 2 * doubles + 3 * triples + 4 * home_runs) / plate_appearances as slugging_percentage
        , strikeouts / plate_appearances as strikeout_percentage
        , walks / plate_appearances as walk_percentage
        , (singles + doubles + triples) / (at_bats - home_runs - strikeouts) as babip
        from totals
    )
    select player_id
    , yr
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
    , case when batting_average = 'nan' then 0 else batting_average end as batting_average
    , case when on_base_percentage = 'nan' then 0 else on_base_percentage end as on_base_percentage
    , case when slugging_percentage = 'nan' then 0 else slugging_percentage end as slugging_percentage
    , case when strikeout_percentage = 'nan' then 0 else strikeout_percentage end as strikeout_percentage
    , case when walk_percentage = 'nan' then 0 else walk_percentage end as walk_percentage
    , case when babip = 'nan' then 0 else babip end as babip
    , case
        when slugging_percentage - batting_average = 'nan'
        then 0
        else slugging_percentage - batting_average
    end as isolated_power
    , case
        when on_base_percentage + slugging_percentage = 'nan'
        then 0
        else on_base_percentage + slugging_percentage
    end as ops
    from averages
)
to 'data/publish/batting_yearly'
(
    format parquet,
    partition_by (yr, player_id),
    overwrite true
)
;