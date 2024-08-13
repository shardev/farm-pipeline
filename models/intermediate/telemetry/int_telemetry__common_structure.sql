select
    vehicleid,
    timestamp,
    gpslongitude,
    gpslatitude,
    speedkmh,
    null as totalworkinghours,
    null as enginerpm,
    null as outdoortempc,
    'wialon' as source
from {{ ref("stg_wialon__telemetry_format") }}
union all
select
    vehicleid,
    timestamp,
    gpslongitude,
    gpslatitude,
    speedkmh,
    totalworkinghours,
    enginerpm,
    outdoortempc,
    'telematics' as source
from {{ ref("stg_telematics__telemetry_format") }}
union all
select
    vehicleid,
    timestamp,
    gpslongitude,
    gpslatitude,
    speedkmh,
    totalworkinghours,
    enginerpm,
    outdoortempc,
    'fendt' as source
from {{ ref("int_telemetry__fendt_joined") }}
