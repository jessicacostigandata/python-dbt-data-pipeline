{{ config(materialized='view') }}

select
  order_id,
  user_id,
  order_total,
  created_at
from {{ source('raw','orders') }}