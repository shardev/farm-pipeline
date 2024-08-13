select
    machineid as vehicleid,
    timestamp_seconds(cast(route.t as int64)) as timestamp,
    route.lng as gpslongitude,
    route.lat as gpslatitude
from {{ source("fendt", "stg_fendt__telemetry_gps") }}, unnest(route) as route
