select *
from 'data/build/players/*/*.parquet'
where player_id = 'choij001'
limit 10
;