{% set vehicle_run_delay_limit = 1000 %}
{% set seconds_in_hour = 3600 %}

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
            lead(gpslatitude) over (
                partition by vehicleid order by timestamp
            ) as next_latitude,
            lead(gpslongitude) over (
                partition by vehicleid order by timestamp
            ) as next_longitude,
            sum(
                case
                    when time_diff_in_seconds > {{ vehicle_run_delay_limit }}
                    then 1
                    else 0
                end
            ) over (partition by vehicleid order by timestamp)
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
                -- Haversine formula with clamping to avoid floating point errors
                6371000 * acos(
                    greatest(
                        least(
                            cos(gpslatitude * acos(-1) / 180)
                            * cos(next_latitude * acos(-1) / 180)
                            * cos(
                                next_longitude * acos(-1) / 180
                                - gpslongitude * acos(-1) / 180
                            )
                            + sin(gpslatitude * acos(-1) / 180)
                            * sin(next_latitude * acos(-1) / 180),
                            1
                        ),
                        -1
                    )
                )
            ) over (partition by vehicleid, run_number_for_machine)
            * 0.001 as distance_km_traveled,
            sum(
                (
                    case
                        when time_diff_in_seconds > {{ vehicle_run_delay_limit }}
                        then 0
                        else time_diff_in_seconds
                    end
                    / {{ seconds_in_hour }}
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
