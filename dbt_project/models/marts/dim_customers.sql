{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='email',
        tags=['db_customers']
    )
}}

with legacy as (

    select
        customer_id,
        lower(trim(email)) as email,
        split(full_name, ' ')[offset(0)] as first_name,
        trim(substr(full_name, strpos(full_name, ' ') + 1)) as last_name,
        cast(signup_date as timestamp) as created_at,
        account_status,
        updated_at,
        _ingested_at,
        'legacy' as source_system
    from {{ ref('stg_legacy_customers') }}

),

modern as (

    select
        customer_id,
        lower(trim(email)) as email,
        first_name,
        last_name,
        created_at,
        account_status,
        updated_at,
        _ingested_at,
        'modern' as source_system
    from {{ ref('stg_modern_customers') }}

),

all_records as (

    select * from legacy
    union all
    select * from modern

),

{% if is_incremental() %}
changed_emails as (

    select distinct email
    from all_records
    where _ingested_at > (select coalesce(max(_ingested_at), timestamp '1970-01-01') from {{ this }})

),
{% endif %}

scoped as (

    select *
    from all_records
    {% if is_incremental() %}
    where email in (select email from changed_emails)
    {% endif %}

),

latest_record as (

    select
        email,
        first_name,
        last_name,
        account_status,
        updated_at
    from scoped
    qualify row_number() over (
        partition by email
        order by updated_at desc, _ingested_at desc
    ) = 1

),

rolled_up as (

    select
        email,
        min(created_at) as created_at,
        string_agg(
            distinct if(source_system = 'legacy', customer_id, null), ', '
            order by if(source_system = 'legacy', customer_id, null)
        ) as legacy_customer_ids,
        string_agg(
            distinct if(source_system = 'modern', customer_id, null), ', '
            order by if(source_system = 'modern', customer_id, null)
        ) as modern_customer_ids,
        case
            when count(distinct source_system) = 2 then 'both'
            else max(source_system)
        end as source_systems,
        max(_ingested_at) as _ingested_at
    from scoped
    group by email

)

select
    l.email,
    l.first_name,
    l.last_name,
    r.created_at,
    l.account_status,
    l.updated_at,
    r.legacy_customer_ids,
    r.modern_customer_ids,
    r.source_systems,
    r._ingested_at,
    current_timestamp() as computed_at
from latest_record l
join rolled_up r
    on l.email = r.email