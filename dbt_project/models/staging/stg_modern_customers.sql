{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='customer_id',
        tags=['db_customers']
    )
}}

with source as (

    select *
    from {{ source('raw', 'modern_customers') }}

    {% if is_incremental() %}
    where updated_at > (select coalesce(max(updated_at), timestamp '1970-01-01') from {{ this }})
    {% endif %}

),

renamed as (

    select
        id as customer_id,
        first_name,
        last_name,
        email_address as email,
        created_at,
        status as account_status,
        updated_at,
        ingested_at as _ingested_at
    from source

)

select *
from renamed
qualify row_number() over (
    partition by customer_id
    order by updated_at desc, _ingested_at desc
) = 1