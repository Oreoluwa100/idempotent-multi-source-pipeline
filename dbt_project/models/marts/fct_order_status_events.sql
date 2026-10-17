{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='event_id',
        tags=['api_events']
    )
}}

select
    event_id,
    order_id,
    status,
    status_changed_at,
    date(status_changed_at) as status_date,
    updated_by,
    case
        when updated_by = 'system_auto' then 'system'
        when starts_with(updated_by, 'agent_') then 'agent'
        else 'unknown'
    end as changed_by_type,
    carrier_name,
    _ingested_at,
    current_timestamp() as computed_at
from {{ ref('stg_api_order_status_events') }}

{% if is_incremental() %}
where _ingested_at > (select coalesce(max(_ingested_at), timestamp '1970-01-01') from {{ this }})
{% endif %}