{{
  config(
    materialized = 'table',
    )
}}
select
    ticket_no,
    flight_id,
    fare_conditions,
    amount,
    current_date as load_date
from
    {{ ref('stg_flights__ticket_flights') }}