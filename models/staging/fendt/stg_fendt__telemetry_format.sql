{% set types = [
    {"type": "WheelBasedVehicleSpeed", "alias": "speed_kmh"},
    {"type": "TOTAL_VEHICLE_HOURS", "alias": "totalworkinghours"},
    {"type": "EngineSpeed", "alias": "engine_rpm"},
    {"type": "OutdoorTemp", "alias": "outdoortemp_c"},
    {"type": "FuelRate", "alias": "fuelconsumption_h"},
] %}

with
    flattened_data as (
        select
            cast(machineid as int64) as vehicleid,
            timestamp_seconds(cast(values.timestamp as int64)) as timestamp,
            datas.type as type,
            cast(values.value as float64) as value
        from
            {{ source("fendt", "stg_fendt__telemetry") }},
            unnest(datas) as datas,
            unnest(datas.values) as values
        where
            values.value is not null
    )
select
    vehicleid,
    timestamp,
    {% for t in types %}
        max(case when type = '{{ t.type }}' then value end) as {{ t.alias }}
        {% if not loop.last %},{% endif %}
    {% endfor %}
from flattened_data
group by vehicleid, timestamp
order by vehicleid, timestamp
