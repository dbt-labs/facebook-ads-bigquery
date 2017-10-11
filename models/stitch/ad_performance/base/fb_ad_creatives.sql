with creatives as (

    select
        id,
        object_id,
        object_story_id,
        object_story_spec,
        object_type,

        name,
        status,
        title,

        account_id,
        body,
        call_to_action_type,
        effective_instagram_story_id,
        effective_object_story_id,
        image_hash,
        image_url,
        instagram_actor_id,
        instagram_permalink_url,
        link_og_id,
        link_url,
        thumbnail_url,
        url_tags,
        use_page_actor_override,
        video_id,

        _sdc_received_at as updated_at

    from {{ var('ad_creatives_table') }}

),

dedupe as (

    select *,
        row_number() over (partition by id order by updated_at desc) as dedupe_index

    from creatives

)

select *
from dedupe
where dedupe_index = 1
