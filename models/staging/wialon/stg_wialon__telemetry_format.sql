select
    unit_id as vehicleid,
    timestamp(datetime) as timestamp,
    gpslongitude as gpslongitude,
    gpslatitude as gpslatitude,
    speed as speed_kmh
from {{ source("wialon", "stg_wialon__telemetry") }}
