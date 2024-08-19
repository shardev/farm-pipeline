{{ config(materialized="table") }}

with
    gps_points_vehicle_runs_assigned as (
        select
            telemetry.vehicleid,
            vehicle_runs.run_number_for_machine,
            telemetry.timestamp,
            vehicle_runs.runstart,
            vehicle_runs.runend,
            st_geogpoint(telemetry.gpslongitude, telemetry.gpslatitude) as point
        from {{ ref("int_telemetry__common_structure") }} as telemetry
        inner join
            {{ ref("int_vehicle_runs__aggregation") }} as vehicle_runs
            on telemetry.vehicleid = vehicle_runs.vehicleid
            and telemetry.timestamp >= vehicle_runs.runstart
            and telemetry.timestamp <= vehicle_runs.runend
    ),

    convex_hull_per_vehicle_run as (
        select
            vehicleid,
            run_number_for_machine,
            max(runstart) as timestart,
            max(runend) as timeend,
            st_convexhull(st_union_agg(point)) as coveragemap
        from gps_points_vehicle_runs_assigned
        group by vehicleid, run_number_for_machine
    )

select
    vehicleid,
    run_number_for_machine,
    timestart,
    timeend,
    coveragemap,
    st_asgeojson(coveragemap) as geojson_coveragemap
from convex_hull_per_vehicle_run
order by vehicleid, run_number_for_machine
