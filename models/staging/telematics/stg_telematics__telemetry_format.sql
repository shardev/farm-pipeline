select
    safe_cast(substr(serialnumber, 2, length(datetime)) as integer) as vehicleid,
    datetime as timestamp,
    gpslongitude,
    gpslatitude,
    speedradar_km_h as speedkmh,
    totalworkinghours,
    engine_rpm as enginerpm,
    tempambient_c as outdoortempc
from `farm-datapipeline.dbt_asarovic.stg_telematics__telemetry`
