{{
  config(
    materialized = 'table',
    )
}}
select
    flight_id,
    flight_no,
    scheduled_departure,
    scheduled_arrival,
    departure_airport,
    arrival_airport,
    status,
    aircraft_code,
    actual_departure,
    actual_arrival,
    current_date as load_date,
    {{concat_columns(['flight_id', 'flight_no'])}} as flight_id_no
from
    {{ ref('stg_flights__flights') }}