select
    unit_id as vehicleid,
    timestamp(datetime) as timestamp,
    gpslongitude as gpslongitude,
    gpslatitude as gpslatitude,
    speed as speedkmh
from {{ source("wialon", "stg_wialon__telemetry") }}
