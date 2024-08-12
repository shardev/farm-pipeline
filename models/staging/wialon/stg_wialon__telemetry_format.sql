select
    unit_id as vehicleid,
    timestamp(datetime) as timestamp,
    gpslongitude as gpslongitude,
    gpslatitude as gpslatitude,
    speed as speedkmh
from `farm-datapipeline.dbt_asarovic.stg_wialon__telemetry`
