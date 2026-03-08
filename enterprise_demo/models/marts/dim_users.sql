{{ config(
    materialized='incremental',
    unique_key='id'
) }}

select
    id,
    email,
    created_at,
    updated_at,
    ingested_at
from {{ source('raw', 'users') }}

{% if is_incremental() %}
where updated_at > (
    select coalesce(max(updated_at), '1900-01-01'::timestamp)
    from {{ this }}
)
{% endif %}