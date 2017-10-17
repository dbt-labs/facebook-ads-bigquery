
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

paid_metrics as (

    select
        *,
        sum(case when action_type = 'post' then num_actions else null end) over
            (partition by ad_id, date_day order by date_day rows between unbounded preceding and unbounded following)
            as paid_shares,
        sum(case when action_type = 'like' then num_actions else null end) over
            (partition by ad_id, date_day order by date_day rows between unbounded preceding and unbounded following)
            as paid_likes
    from joined
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16

),

max_values as (
--there are situations where a singular adset_id shows up multiple times
--per action type per day.

    select distinct
        date_day,
        campaign_id,
        ad_id,
        adset_id,
        account_id,
        account_name,
        ad_unique_id,
        adset_name,
        campaign_name,
        paid_shares,
        paid_likes,
        first_value(object_story_id ignore nulls) over (partition by ad_id
            order by date_day rows between unbounded preceding and unbounded
            following) as object_story_id,
        max(impressions) over (partition by ad_id, date_day order by date_day
            rows between unbounded preceding and unbounded following)
            as impressions,
        max(spend) over (partition by ad_id, date_day order by date_day
            rows between unbounded preceding and unbounded following) as spend,
        max(clicks) over (partition by ad_id, date_day order by date_day
            rows between unbounded preceding and unbounded following) as clicks,
        sum(num_actions) over (partition by ad_id, date_day
            order by date_day rows between unbounded preceding and unbounded
            following) as num_actions
    from paid_metrics

),

final as (

    select

      *,
      to_base64(sha1(concat(
        cast(date_day as string),
        cast(adset_id as string),
        cast(impressions as string),
        cast(spend as string)
        ))) as id
    from max_values
)

select * from final
