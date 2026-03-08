{{ config(materialized='table') }}

-- fact table representing aggregated user revenue
-- grain: 1 row per user
-- built from stg_users and stg_orders

select
  u.user_id,  -- 1
  u.email,    -- 2
  count(o.order_id) as order_count,
  sum(o.order_total) as lifetime_revenue
from {{ ref('stg_users') }} u
left join {{ ref('stg_orders') }} o
  on o.user_id = u.user_id
group by 1,2  -- = group by u.user_id, u.email  (Grain: 1 row per user_id)