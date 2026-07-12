{{
    config(
        materialized='incremental',
        unique_key='trip_id',
        incremental_strategy='delete+insert'
    )
}}

with enriched as (

    select * from {{ ref('int_trips_enriched') }}

    {% if is_incremental() %}
    -- only process trips newer than the latest pickup already loaded
    where pickup_at > (select coalesce(max(pickup_at), '1900-01-01') from {{ this }})
    {% endif %}

)

select
    trip_id,
    vendor_id,
    pickup_at,
    dropoff_at,
    pickup_date,
    pickup_hour,
    duration_minutes,
    passenger_count,
    trip_distance,
    pickup_location_id,
    pickup_borough,
    dropoff_location_id,
    dropoff_borough,
    payment_type_id,
    fare_amount,
    tip_amount,
    tip_pct,
    tolls_amount,
    total_amount
from enriched
