select
    machineid as vehicleid,
    TIMESTAMP_SECONDS(CAST(route.t AS INT64)) AS timestamp,
    route.lng as gpslongitude,
    route.lat as gpslatitude
from `farm-datapipeline.dbt_asarovic.stg_fendt__telemetry_gps`, unnest(route) as route
