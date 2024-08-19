select
    vehicleid,
    timestamp,
    gpslongitude,
    gpslatitude,
    speed_kmh,
    null as totalworkinghours,
    null as engine_rpm,
    null as outdoortemp_c,
    null as fuelconsumption_h,
    'wialon' as source
from {{ ref("stg_wialon__telemetry_format") }}
where gpslongitude is not null and gpslatitude is not null
union all
select
    vehicleid,
    timestamp,
    gpslongitude,
    gpslatitude,
    speed_kmh,
    totalworkinghours,
    engine_rpm,
    outdoortemp_c,
    fuelconsumption_h,
    'telematics' as source
from {{ ref("stg_telematics__telemetry_format") }}
where gpslongitude is not null and gpslatitude is not null
union all
select
    vehicleid,
    timestamp,
    gpslongitude,
    gpslatitude,
    speed_kmh,
    totalworkinghours,
    engine_rpm,
    outdoortemp_c,
    fuelconsumption_h,
    'fendt' as source
from {{ ref("int_telemetry__fendt_joined") }}
where gpslongitude is not null and gpslatitude is not null
