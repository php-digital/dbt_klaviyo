{{
    config(
        materialized='incremental',
        unique_key='unique_event_id',
        incremental_strategy = 'merge' if target.type not in ('postgres', 'redshift') else 'delete+insert',
        file_format = 'delta',
        partition_by={
            "field": "occurred_at",
            "data_type": "timestamp",
            "granularity": "month"
        }
    )
}}

with events as (
    select 
        variation_id,
        event_id,
        unique_event_id,
        person_id,
        metric_id,
        uuid,
        numeric_value,
        occurred_at,
        type,
        campaign_id,
        flow_id,
        flow_message_id,
        _fivetran_synced,
        coalesce(campaign_id, flow_id) as touch_id,
        case 
            when campaign_id is not null then 'campaign' 
            when flow_id is not null then 'flow' 
        else null end as touch_type,
        timestamp_trunc(occurred_at, MONTH) as event_month

    from {{ var('event_table') }}

    {% if is_incremental() %}
    where _fivetran_synced >= cast(coalesce( 
        (
            select {{ dbt.dateadd(datepart = 'hour', 
                                        interval = -1,
                                        from_date_or_timestamp = 'max(_fivetran_synced)' ) }}  
            from {{ this }}
        ), '2012-01-01') as {{ dbt.type_timestamp() }})
    {% endif %}
),

create_sessions as (
    select
        variation_id,
        unique_event_id,
        event_id,
        person_id,
        metric_id,
        uuid,
        numeric_value,
        occurred_at,
        type,
        campaign_id,
        flow_id,
        flow_message_id,
        touch_id,
        touch_type,
        event_month,
        sum(case when touch_id is not null
        {% if var('klaviyo__eligible_attribution_events') != [] %}
            and lower(type) in {{ "('" ~ (var('klaviyo__eligible_attribution_events') | join("', '")) ~ "')" }}
        {% endif %}
            then 1 else 0 end) over (
                partition by person_id, event_month 
                order by occurred_at asc 
                rows between unbounded preceding and current row) as touch_session 
    from events
),

attribution as (
    select 
        cs.*,
        min(occurred_at) over(
            partition by person_id, event_month, touch_session) as session_start_at,
        first_value(type) over(
            partition by person_id, event_month, touch_session 
            order by occurred_at asc 
            rows between unbounded preceding and current row) as session_event_type,
        coalesce(
            touch_id,
            first_value(case when touch_id is not null then touch_id else null end) over(
                partition by person_id, event_month, touch_session 
                order by occurred_at asc 
                rows between unbounded preceding and current row)
        ) as last_touch_id,
        coalesce(
            touch_type,
            first_value(case when touch_type is not null then touch_type else null end) over(
                partition by person_id, event_month, touch_session 
                order by occurred_at asc 
                rows between unbounded preceding and current row)
        ) as session_touch_type
    from create_sessions cs
),

final as (
    select
        variation_id,
        unique_event_id,
        event_id,
        metric_id,
        uuid,
        numeric_value,
        person_id,
        occurred_at,
        type,
        campaign_id,
        flow_id,
        flow_message_id,
        touch_id,
        touch_type,
        event_month,
        touch_session,
        session_start_at,
        session_event_type,
        last_touch_id,
        session_touch_type,
        case when {{ dbt.datediff('session_start_at', 'occurred_at', 'hour') }} <= 
            case when lower(session_event_type) like '%sms%' 
                then {{ var('klaviyo__sms_attribution_lookback') }}
                else {{ var('klaviyo__email_attribution_lookback') }} 
            end
        then last_touch_id
        else null
        end as attributed_touch_id
    from attribution
)

select * from final