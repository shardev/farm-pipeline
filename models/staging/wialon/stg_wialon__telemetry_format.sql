select
    unit_id as vehicleid,
    safe_cast(substr(datetime, 1, length(datetime) - 4) as datetime) as timestamp,
    gpslongitude as gpslongitude,
    gpslatitude as gpslatitude,
    speed as speedkmh
from `farm-datapipeline.dbt_asarovic.stg_wialon__telemetry`
