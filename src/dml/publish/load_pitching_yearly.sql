copy (
    with totals as (
        select player_id
        , yr
        , sum(is_starter) as starts
        , sum(outs_recorded) as outs_recorded
        , sum(batters_faced) as batters_faced
        , sum(singles) as singles
        , sum(doubles) as doubles
        , sum(triples) as triples
        , sum(home_runs) as home_runs
        , sum(earned_runs) as earned_runs
        , sum(unearned_runs) as unearned_runs
        , sum(walks) as walks
        , sum(intentional_walks) as intentional_walks
        , sum(strikeouts) as strikeouts
        , sum(hit_by_pitches) as hit_by_pitches
        , sum(wild_pitches) as wild_pitches
        , sum(passed_balls) as passed_balls
        , sum(balks) as balks
        , sum(sacrifice_hits) as sacrifice_hits
        , sum(sacrifice_flies) as sacrifice_flies
        , sum(stolen_bases) as stolen_bases
        , sum(caught_stealing) as caught_stealing
        , sum(case when award = 'win' then 1 else 0 end) as wins
        , sum(case when award = 'loss' then 1 else 0 end) as losses
        , sum(case when award = 'save' then 1 else 0 end) as saves
        , sum(complete_game) as complete_games
        from 'data/build/pitching_games/*/*/*/*.parquet'
        where yr between {{ start }} and {{ end }}
        group by player_id, yr
    )
    select player_id
    , yr
    , starts
    , outs_recorded
    , batters_faced
    , singles
    , doubles
    , triples
    , home_runs
    , earned_runs
    , unearned_runs
    , walks
    , intentional_walks
    , strikeouts
    , hit_by_pitches
    , wild_pitches
    , passed_balls
    , balks
    , sacrifice_hits
    , sacrifice_flies
    , stolen_bases
    , caught_stealing
    , wins
    , losses
    , saves
    , complete_games
    , nullif(27 * earned_runs / outs_recorded, 'nan') as earned_run_average
    , strikeouts / batters_faced as strikeout_percentage
    , walks / batters_faced as walk_percentage
    , (outs_recorded // 3)::varchar || '.' || (outs_recorded % 3)::varchar as innings_pitched
    from totals
)
to 'data/publish/pitching_yearly'
(
    format parquet,
    partition_by (yr, player_id),
    overwrite true
)
;