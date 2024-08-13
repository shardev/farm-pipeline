select
    safe_cast(substr(serialnumber, 2, length(datetime)) as integer) as vehicleid,
    timestamp(datetime) as timestamp,
    gpslongitude,
    gpslatitude,
    speedradar_km_h as speedkmh,
    totalworkinghours,
    engine_rpm as enginerpm,
    tempambient_c as outdoortempc
from {{ source("telematics", "stg_telematics__telemetry") }}
