copy (
    with totals as (
        select team_id
        , yr
        , sum(win) as wins
        , sum(loss) as losses
        , sum(tie) as ties
        , sum(plate_appearances) as plate_appearances
        , sum(at_bats) as at_bats
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
        , sum(batters_faced) as batters_faced
        , sum(singles_allowed) as singles_allowed
        , sum(doubles_allowed) as doubles_allowed
        , sum(triples_allowed) as triples_allowed
        , sum(home_runs_allowed) as home_runs_allowed
        , sum(earned_runs_allowed) as earned_runs_allowed
        , sum(unearned_runs_allowed) as unearned_runs_allowed
        , sum(walks_allowed) as walks_allowed
        , sum(intentional_walks_allowed) as intentional_walks_allowed
        , sum(pitcher_strikeouts) as pitcher_strikeouts
        , sum(hit_by_pitches_allowed) as hit_by_pitches_allowed
        , sum(wild_pitches) as wild_pitches
        , sum(passed_balls) as passed_balls
        , sum(balks_allowed) as balks_allowed
        , sum(sacrifice_hits_allowed) as sacrifice_hits_allowed
        , sum(sacrifice_flies_allowed) as sacrifice_flies_allowed
        , sum(stolen_bases_allowed) as stolen_bases_allowed
        , sum(pitcher_caught_stealing) as pitcher_caught_stealing
        , sum(putouts) as putouts
        , sum(assists) as assists
        , sum(errors) as errors
        , sum(pitcher_double_plays) as pitcher_double_plays
        , sum(pitcher_triple_plays) as pitcher_triple_plays
        , sum(runs_scored) as runs_scored
        , sum(runs_allowed) as runs_allowed
        from 'data/build/team_games/*/*/*.parquet'
        where yr between {{ start }} and {{ end }}
        group by team_id, yr
    )
    select team_id
    , yr
    , wins
    , losses
    , ties
    , plate_appearances
    , at_bats
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
    , batters_faced
    , singles_allowed
    , doubles_allowed
    , triples_allowed
    , home_runs_allowed
    , earned_runs_allowed
    , unearned_runs_allowed
    , walks_allowed
    , intentional_walks_allowed
    , pitcher_strikeouts
    , hit_by_pitches_allowed
    , wild_pitches
    , passed_balls
    , balks_allowed
    , sacrifice_hits_allowed
    , sacrifice_flies_allowed
    , stolen_bases_allowed
    , pitcher_caught_stealing
    , putouts
    , assists
    , errors
    , pitcher_double_plays
    , pitcher_triple_plays
    , runs_scored
    , runs_allowed
    from totals
)
to 'data/publish/teams_yearly'
(
    format parquet,
    partition_by (yr),
    overwrite true
)
;