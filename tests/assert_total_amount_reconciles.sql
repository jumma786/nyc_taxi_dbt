-- Reconciliation test: total_amount should be at least the sum of its
-- major components (fare + tip + tolls). Returns rows that FAIL.
-- A small tolerance covers surcharges/taxes not modelled here.
select
    trip_id,
    total_amount,
    fare_amount + tip_amount + tolls_amount as component_sum
from {{ ref('fct_trips') }}
where total_amount + 0.01 < (fare_amount + tip_amount + tolls_amount)
