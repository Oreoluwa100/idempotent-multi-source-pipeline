{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='order_id',
        tags=['csv_orders']
    )
}}

with source as (

    select *
    from {{ source('raw', 'csv_source_orders') }}

    {% if is_incremental() %}
    where ingested_at > (select coalesce(max(_ingested_at), timestamp '1970-01-01') from {{ this }})
    {% endif %}

),

renamed as (

    select
        order_id,
        customer_id,
        product_category,
        amount,
        payment_method,
        event_timestamp,
        ingested_at as _ingested_at,
        source_file as _source_file
    from source

)

select *
from renamed
qualify row_number() over (
    partition by order_id
    order by _ingested_at desc
) = 1