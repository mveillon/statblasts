create table if not exists build.plays
(
    play_id string
    , game_id string
    , inning int
    , is_bottom_inning int
    , is_visiting_batting int
    , batter string
    , pitcher string
    , outs_pre int
    , bs_count string
    , num_pitches int
    , was_pa int
    , was_ab int
    , was_out int
    , event_type string
    , hit_trajectory string
    , base_attempted_steal_at int
    , error_positions int[]
    , on_first_pre string
    , on_second_pre string
    , on_third_pre string
    , on_first_post string
    , on_second_post string
    , on_third_post string
    , pitcher_charged_batter string
    , pitcher_charged_first string
    , pitcher_charged_second string
    , pitcher_charged_third string
    , rbi int
    , earned_runs int
    , unearned_runs int
    , fielder string
    , gametype string
    , yr int
    , mo int
    , dy int
    , primary key (play_id)
)
;