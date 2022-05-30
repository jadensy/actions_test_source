WITH date_select AS (
    SELECT
        [daterange_start] start_date,
        [daterange_end] end_date
),
--------------------------------
-- registered users
--------------------------------
lite_membership AS (
    SELECT
        a.userid,
        a.email,
        a.source,
        case when a.source_breakdown='uncategorized' then b.platform else a.source_breakdown end as source_app,
        a.first_date AS lite_date,
        a.first_timestamp AS lite_datetime
    FROM
        marketing.stg_user__membership__merged a
        left join marketing.fct_install_attribution_model__install_last_touch b on a.email=b.user_email and a.first_timestamp=b.lite_timestamp and a.product=b.lite 
        CROSS JOIN date_select
    WHERE
        product = 'lite'
        AND first_date >= date_select.start_date
        AND first_date <= date_select.end_date
),
--------------------------------
-- core users
--------------------------------
core_membership AS (
    SELECT
        a.userid,
        a.email,
        a.source,
        case when a.source_breakdown='uncategorized' then b.platform else a.source_breakdown end as source_app,
        a.first_date AS core_date,
        a.first_timestamp AS core_datetime
    FROM
        marketing.stg_user__membership__merged a
        left join marketing.fct_install_attribution_model__install_last_touch b on a.email=b.user_email and a.first_timestamp=b.core_timestamp and a.product=b.core 
        CROSS JOIN date_select
    WHERE
        product = 'new core'
        AND first_date >= date_select.start_date
        AND first_date <= date_select.end_date
),
--------------------------------
-- ffa user
--------------------------------
ffa_membership AS (
    SELECT
        a.userid,
        a.first_date AS ffa_date,
        a.first_timestamp AS ffa_datetime,
        case when a.source_breakdown='uncategorized' then b.platform else a.source_breakdown end as source_app
    FROM
        marketing.stg_user__membership__merged_with_aff a
   left join marketing.fct_install_attribution_model__install_last_touch b on a.email=b.user_email and a.first_timestamp=b.ffa_timestamp and a.product=b.ffa_product 
    
    WHERE
        product NOT IN ('lite', 'new core')
        AND rn = 1
    ),
--------------------------------
-- install campaign
--------------------------------
install_campaign_user AS (
    SELECT
        userid,
        email AS user_email,
        product_su,
        product_su_date,
        product_su_datetime
    FROM
        marketing.fct_attribution_model__product_last_touch
        CROSS JOIN date_select
    WHERE
        product_su = 'new core'
        AND source = 'performance marketing'
        AND ad_platform IN (
            'blindferret',
            'tapjoy'
        )
        AND product_su_date >= date_select.start_date
        AND product_su_date <= date_select.end_date
),
--------------------------------
-- core to ffa
--------------------------------
core_to_ffa AS (
    SELECT
        core_membership.userid as core_userid,
        core_membership.source,
        core_membership.core_date,
        ffa_membership.ffa_date,
        ffa_membership.userid as ffa_userid,
        coalesce(core_membership.source_app,lite_membership.source_app, ffa_membership.source_app) as app_platform,
        ------ transformation ------
        case when install_campaign_user.userid is not null then 1 else 0 end is_install_campaign
    FROM
        core_membership
        LEFT JOIN lite_membership ON lite_membership.userid = core_membership.userid
        LEFT JOIN ffa_membership ON core_membership.userid = ffa_membership.userid
        LEFT JOIN install_campaign_user ON core_membership.userid = install_campaign_user.userid
    where 
        core_membership.source = 'mobileApp'
        and is_install_campaign = 0
        and lower(app_platform)='ios'
),
result AS (
    SELECT
        [core_date:aggregation] dt,
        count(distinct core_userid) core_count,
        count(distinct ffa_userid) ffa_count
    FROM
        core_to_ffa
    GROUP BY
        1
),
final AS (
    SELECT
        dt,
        core_count core,
        ffa_count ffa,
        (1.00 * ffa_count / core_count) core_to_ffa
    FROM
        result
)
SELECT
    *
FROM
    final
ORDER BY
    1 DESC