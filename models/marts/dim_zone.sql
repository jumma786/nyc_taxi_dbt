with zones as (

    select * from {{ ref('stg_zones') }}

)

select
    location_id,
    borough,
    zone,
    service_zone
from zones
