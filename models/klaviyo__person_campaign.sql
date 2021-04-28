with person_campaign as (
    
    select *
    from {{ ref('int_klaviyo__person_campaign_flow') }}

    where last_touch_campaign_id is not null
),

fields as (

    {% set exclude_fields = ['flow_name', 'last_touch_flow_id', 'last_touch_campaign_id'] %}
    {% set exclude_fields = exclude_fields | upper if target.type == 'snowflake' else exclude_fields %}

    select
        last_touch_campaign_id as campaign_id,
        {{ dbt_utils.star(from=ref('int_klaviyo__person_campaign_flow'), except=exclude_fields) }}

    from person_campaign
)

select *
from fields