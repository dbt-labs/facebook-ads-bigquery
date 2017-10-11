
with ads as (

  select * from {{ref('fb_ads_xf')}}

), creatives as (

  select * from {{ref('fb_ad_creatives')}}

), insights as (

  select * from {{ref('fb_ad_insights')}}

), adsets as (

  select * from {{ref('fb_adsets')}}

), campaigns as (

    select * from {{ref('fb_campaigns')}}

),

base as (

    select distinct
        --md5(concat(cast(insights.date_day as string), '|', cast(ads.unique_id as string))) as id,
        insights.*,
        creatives.object_story_id,
        ads.unique_id as ad_unique_id
    from insights
    left outer join ads
    on insights.ad_id = ads.id
    and insights.date_day >= date_trunc(date (ads.effective_from), day)
    and (insights.date_day < date_trunc(date (ads.effective_to), day) or ads.effective_to is null)
    left outer join creatives on ads.creative_id = creatives.id

  --these have to be an outer join because while the stitch integration goes
  --back in time for the core reporting tables (insights, etc),
  --it doesn't go back in time for the lookup tables. so there are actually lots
  --of ad_ids that don't exist when you try to do the join,
  --but they're always prior to the date you initially made the connection.

),

joined as (

    select
        base.*,
        adsets.name as adset_name,
        campaigns.name as campaign_name
    from base
    left join adsets
        on base.adset_id = adsets.id
    left join campaigns
        on base.campaign_id = campaigns.id

),

max_values as (
--there are situations where a singular adset_id shows up multiple times
--per action type per day. In comparing to the values in the existing document
--https://docs.google.com/spreadsheets/d/1RrYGNpAK1n6SyZlTTFoECXQ7jqMyNmFYq6giWpPo5Hw/edit?ts=5967d5a7#gid=1134602971
--grabbing the max values for actions and spend matched the values perfectly

    select distinct
        date_day,
        campaign_id,
        ad_id,
        adset_id,
        account_id,
        account_name,
        ad_unique_id,
        action_type,
        adset_name,
        campaign_name,
        first_value(object_story_id ignore nulls) over (partition by adset_id
            order by date_day rows between unbounded preceding and unbounded
            following) as object_story_id,
        max(impressions) over (partition by adset_id, date_day order by date_day
            rows between unbounded preceding and unbounded following)
            as impressions,
        max(spend) over (partition by adset_id, date_day order by date_day
            rows between unbounded preceding and unbounded following) as spend,
        max(clicks) over (partition by adset_id, date_day order by date_day
            rows between unbounded preceding and unbounded following) as clicks,
        max(num_actions) over (partition by adset_id, date_day, action_type
            order by date_day rows between unbounded preceding and unbounded
            following) as num_actions
    from joined


),
final as (

    select
        date_day,
        campaign_id,
        adset_id,
        account_id,
        account_name,
        adset_name,
        campaign_name,
        object_story_id,

        sum(case when action_type = 'post' then num_actions else null end)
            as paid_shares,
        sum(case when action_type = 'like' then num_actions else null end)
            as paid_likes,

        max(impressions) as impressions,
        max(clicks) as clicks,
        max(spend) as spend,

        array_agg(struct(distinct 
            ad_id,
            ad_unique_id
        ) order by ad_id) as ads

    from max_values
    group by 1, 2, 3, 4, 5, 6, 7, 8

)

select *,
to_base64(sha1(concat(cast(date_day as string), cast(adset_id as string)))) as id
from final
