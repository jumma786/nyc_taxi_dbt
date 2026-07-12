with trips as (

    select * from {{ ref('stg_trips') }}

),

zones as (

    select * from {{ ref('stg_zones') }}

),

enriched as (

    select
        t.trip_id,
        t.vendor_id,
        t.pickup_at,
        t.dropoff_at,
        t.passenger_count,
        t.trip_distance,

        -- pickup zone attributes
        t.pickup_location_id,
        pu.borough      as pickup_borough,
        pu.zone         as pickup_zone,

        -- dropoff zone attributes
        t.dropoff_location_id,
        dof.borough      as dropoff_borough,
        dof.zone         as dropoff_zone,

        t.payment_type_id,
        t.fare_amount,
        t.tip_amount,
        t.tolls_amount,
        t.total_amount,

        -- derived metrics
        date_trunc('day', t.pickup_at)                              as pickup_date,
        extract(hour from t.pickup_at)                              as pickup_hour,
        {{ trip_duration_minutes('t.pickup_at', 't.dropoff_at') }}  as duration_minutes,
        case
            when t.fare_amount > 0
            then round(t.tip_amount / t.fare_amount, 4)
            else 0
        end                                                         as tip_pct

    from trips t
    left join zones pu on t.pickup_location_id  = pu.location_id
    left join zones dof on t.dropoff_location_id = dof.location_id

)

select * from enriched
