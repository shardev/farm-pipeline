select
    gps.vehicleid,
    gps.timestamp,
    gps.gpslongitude,
    gps.gpslatitude,
    telemetry.speedkmh,
    telemetry.totalworkinghours,
    telemetry.enginerpm,
    telemetry.outdoortempc
from 
    {{ ref('stg_fendt__telemetry_gps_format') }} gps
join
    {{ ref('stg_fendt__telemetry_format') }} telemetry
    on gps.vehicleid = telemetry.vehicleid
    and gps.timestamp = telemetry.timestamp

