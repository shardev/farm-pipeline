with
    timestamps_diff as (
        select
            *,
            timestamp_diff(
                timestamp,
                lag(timestamp) over (partition by vehicleid order by timestamp),
                second
            ) as time_diff_in_seconds
        from {{ ref("int_telemetry__common_structure") }}
    ),
    group_calc as (
        select
            *,
            sum(case when time_diff_in_seconds > 1000 then 1 else 0 end) over (
                partition by vehicleid order by timestamp
            )
            + 1 as run_number_for_machine
        from timestamps_diff
    ),
    group_boundaries as (
        select
            date(
                min(timestamp) over (partition by vehicleid, run_number_for_machine)
            ) as date,
            vehicleid,
            min(timestamp) over (
                partition by vehicleid, run_number_for_machine
            ) as runstart,
            max(timestamp) over (
                partition by vehicleid, run_number_for_machine
            ) as runend,
            run_number_for_machine,

            -- point start
            first_value(gpslongitude) over (
                partition by vehicleid, run_number_for_machine order by timestamp
            ) as run_point_start_lng,
            first_value(gpslatitude) over (
                partition by vehicleid, run_number_for_machine order by timestamp
            ) as run_point_start_lat,

            -- point end
            first_value(gpslongitude) over (
                partition by vehicleid, run_number_for_machine order by timestamp desc
            ) as run_point_end_lng,
            first_value(gpslatitude) over (
                partition by vehicleid, run_number_for_machine order by timestamp desc
            ) as run_point_end_lat,

            sum(
                (
                    case
                        when time_diff_in_seconds > 1000
                        then 0
                        else time_diff_in_seconds
                    end
                    / 3600
                )
                * speed_kmh
            ) over (partition by vehicleid, run_number_for_machine)
            as distance_km_traveled,
            sum(
                (
                    case
                        when time_diff_in_seconds > 1000
                        then 0
                        else time_diff_in_seconds
                    end
                    / 3600
                )
                * fuelconsumption_h
            ) over (partition by vehicleid, run_number_for_machine) as fuel_used_litres
        from group_calc
    ),
    pre_point_created as (
        select distinct *
        from group_boundaries
        order by vehicleid, run_number_for_machine
    )

select
    date,
    vehicleid,
    runstart,
    runend,
    run_number_for_machine,
    st_geogpoint(run_point_start_lng, run_point_start_lat) as runpointstart,
    st_geogpoint(run_point_end_lng, run_point_end_lat) as runpointend,
    distance_km_traveled,
    fuel_used_litres
from pre_point_created
