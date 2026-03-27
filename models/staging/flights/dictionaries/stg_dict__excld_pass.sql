{{
  config(
    materialized = 'table',
    )
}}
select
    passenger_id,
    passenger_name
from 
    {{ref('excld_pass')}}