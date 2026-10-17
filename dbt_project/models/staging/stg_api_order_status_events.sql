{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='event_id'
    )
}}

with source as (

    select *
    from {{ source('raw', 'api_source_events') }}

    {% if is_incremental() %}
    where ingested_at > (select coalesce(max(_ingested_at), timestamp '1970-01-01') from {{ this }})
    {% endif %}

),

renamed as (

    select
        event_id,
        sequence,
        order_id,
        status,
        status_changed_at,
        updated_by,
        carrier_name,
        ingested_at as _ingested_at
    from source

)

select *
from renamed
qualify row_number() over (
    partition by event_id
    order by _ingested_at desc
) = 1