select
    gps.vehicleid,
    gps.timestamp,
    gps.gpslongitude,
    gps.gpslatitude,
    telemetry.speed_kmh,
    telemetry.totalworkinghours,
    telemetry.engine_rpm,
    telemetry.outdoortemp_c,
    telemetry.fuelconsumption_h
from 
    {{ ref('stg_fendt__telemetry_gps_format') }} gps
join
    {{ ref('stg_fendt__telemetry_format') }} telemetry
    on gps.vehicleid = telemetry.vehicleid
    and gps.timestamp = telemetry.timestamp

