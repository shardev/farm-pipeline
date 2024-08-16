select
    safe_cast(substr(serialnumber, 2, length(datetime)) as integer) as vehicleid,
    timestamp(datetime) as timestamp,
    gpslongitude,
    gpslatitude,
    speedradar_km_h as speed_kmh,
    totalworkinghours,
    engine_rpm,
    tempambient_c as outdoortemp_c,
    fuelconsumption_l_h as fuelconsumption_h
from {{ source("telematics", "stg_telematics__telemetry") }}
