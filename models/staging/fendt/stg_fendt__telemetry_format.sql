with
    flattened_data as (
        select
            cast(machineid as int64) as vehicleid,
            -- TIMESTAMP_SECONDS(CAST(values.timestamp AS INT64)) AS timestamp,
            cast(values.timestamp as int64) as timestamp,
            datas.type as type,
            cast(values.value as float64) as value
        from
            `farm-datapipeline.dbt_asarovic.stg_fendt__telemetry`,
            unnest(datas) as datas,
            unnest(datas.values) as
        values
        limit 1000
    )
select
    vehicleid,
    timestamp,
    max(case when type = 'LoadAtCurrSpeed' then value end) as loadatcurrspeed,
    max(case when type = 'PLC_R_Draft' then value end) as plc_r_draft,
    -- Add more types as needed
    max(case when type = 'TotalDEFConsumption' then value end) as totaldefconsumption
from flattened_data
group by vehicleid, timestamp
order by vehicleid, timestamp
