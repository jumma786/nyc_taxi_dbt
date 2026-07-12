with source as (

    select * from {{ source('raw', 'yellow_tripdata') }}

),

renamed as (

    select
        -- surrogate key for a trip (no natural PK in TLC data)
        {{ dbt_utils.generate_surrogate_key([
            'VendorID',
            'tpep_pickup_datetime',
            'tpep_dropoff_datetime',
            'PULocationID',
            'DOLocationID',
            'total_amount'
        ]) }}                                    as trip_id,

        VendorID                                 as vendor_id,
        cast(tpep_pickup_datetime as timestamp)  as pickup_at,
        cast(tpep_dropoff_datetime as timestamp) as dropoff_at,
        cast(passenger_count as integer)         as passenger_count,
        cast(trip_distance as double)            as trip_distance,
        PULocationID                             as pickup_location_id,
        DOLocationID                             as dropoff_location_id,
        payment_type                             as payment_type_id,
        cast(fare_amount as double)              as fare_amount,
        cast(tip_amount as double)               as tip_amount,
        cast(tolls_amount as double)             as tolls_amount,
        cast(total_amount as double)             as total_amount

    from source

),

filtered as (

    select *
    from renamed
    -- basic data-quality gate: drop obviously invalid rows
    where pickup_at is not null
      and dropoff_at is not null
      and dropoff_at >= pickup_at
      and trip_distance >= 0
      and total_amount >= 0
      -- reconciliation gate: total must cover its known components
      -- (a 1-cent tolerance absorbs rounding). Drops a handful of
      -- corrupt TLC rows where total_amount < fare + tip + tolls.
      and total_amount + 0.01 >= fare_amount + tip_amount + tolls_amount

),

deduped as (

    -- the surrogate key can collide when TLC ships duplicate trip rows;
    -- keep one row per trip_id so downstream unique tests hold.
    select *
    from filtered
    qualify row_number() over (
        partition by trip_id
        order by fare_amount desc, tip_amount desc
    ) = 1

)

select * from deduped
