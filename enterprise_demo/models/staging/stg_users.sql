{{ config(materialized='view') }}

select
    id as user_id,
    email,
    created_at
from {{ source('raw', 'users') }}