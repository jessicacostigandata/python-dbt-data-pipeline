{{ config(materialized='view') }}

select *
from {{ ref('stg_users') }}
where created_at >= '2024-01-01'

