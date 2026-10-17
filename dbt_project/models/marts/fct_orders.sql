{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='order_id',
        tags=['csv_orders']
    )
}}

select
    order_id,
    customer_id,
    product_category,
    amount,
    payment_method,
    event_timestamp as ordered_at,
    date(event_timestamp) as order_date,
    _ingested_at,
    current_timestamp() as computed_at
from {{ ref('stg_csv_orders') }}

{% if is_incremental() %}
where _ingested_at > (select coalesce(max(_ingested_at), timestamp '1970-01-01') from {{ this }})
{% endif %}