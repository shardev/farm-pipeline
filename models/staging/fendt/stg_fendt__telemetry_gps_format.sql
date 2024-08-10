select
    machineid as vehicleid,
    route.t as timestamp,
    route.lng as gpslongitude,
    route.lat as gpslatitude
from `farm-datapipeline.dbt_asarovic.stg_fendt__telemetry_gps`, unnest(route) as route
