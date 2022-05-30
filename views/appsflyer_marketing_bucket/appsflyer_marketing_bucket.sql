/*
MAIN: marketing bucket script
this script has been created into a view in periscope
*/


with marketing_bucket as (
    select 'organic' as marketing_bucket, 'blog direct' as marketing_source union all
    select 'affiliate' as marketing_bucket, 'impact' as marketing_source union all
    select 'affiliate' as marketing_bucket, 'leadgen' as marketing_source union all
    select 'organic' as marketing_bucket, 'lifecycle' as marketing_source union all
    select 'organic' as marketing_bucket, 'npm' as marketing_source union all
    select 'organic' as marketing_bucket, 'organic' as marketing_source union all
    select 'paid_brand_media' as marketing_bucket, 'paid brand' as marketing_source union all
    select 'performance_marketing' as marketing_bucket, 'performance marketing' as marketing_source union all
    select 'friend_referral' as marketing_bucket, 'referral' as marketing_source union all
    select 'paid_brand_media' as marketing_bucket, 'social media' as marketing_source union all
    select 'organic' as marketing_bucket, 'uncategorized' as marketing_source union all
    select 'organic' as marketing_bucket, 'website direct' as marketing_source

)

select * from marketing_bucket