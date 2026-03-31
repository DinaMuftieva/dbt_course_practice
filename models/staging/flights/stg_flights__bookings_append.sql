{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    tags = ['bookings']
    )
}}
select
  book_ref,
  book_date,
  {{ curr_conv('total_amount') }} as total_amount
from {{ source('demo_src', 'bookings') }}

{% if is_incremental() %}
where 
    ('0x' || book_ref)::bigint > (select max(('0x' || book_ref)::bigint) from {{this}})
{% endif %}
    