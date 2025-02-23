copy (
    select team_abbr
    , case
        when league = 'A'
        then 'american'
        when league = 'N'
        then 'national'
        else 'unknown'
    end as league
    , location
    , team_name
    , cast(regexp_replace(filename, '[^0-9]', '', 'g') as int) as yr
    from read_csv(
        'data/raw/files_raw/teams/*.csv', -- originally the TEAM<YYYY> files
        names = [
            team_abbr
            , league
            , location
            , team_name
        ],
        header = false,
        filename = true
    )
)
to 'data/build/teams'
(
    format parquet,
    partition_by (yr),
    overwrite true
)
;