
with insights as (

    select * from {{ var('ads_insights_table') }}

),

base as (
--this unnests the actions values to provide one row per day per action type
    select
        date(date_start) as date_day,
        nullif(campaign_id,'') as campaign_id,
        nullif(ad_id,'') as ad_id,
        nullif(adset_id,'') as adset_id,
        nullif(account_id,'') as account_id,
        nullif(account_name,'') as account_name,
        impressions,
        clicks,
        spend,
        value as num_actions,
        nullif(action_type,'') as action_type,
        nullif(action_destination,'') as action_destination
    from insights
    cross join unnest(insights.actions)

)

select * from base
