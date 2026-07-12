with trips as (

    select * from {{ ref('fct_trips') }}

)

select
    pickup_date,
    pickup_borough,
    count(*)                          as trip_count,
    sum(passenger_count)              as total_passengers,
    round(sum(total_amount), 2)       as total_revenue,
    round(avg(total_amount), 2)       as avg_fare,
    round(avg(trip_distance), 2)      as avg_distance,
    round(avg(duration_minutes), 1)   as avg_duration_min,
    round(avg(tip_pct), 4)            as avg_tip_pct
from trips
group by pickup_date, pickup_borough
