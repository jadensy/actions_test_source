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
        userid,
        email,
        source,
        source_breakdown,
        first_date AS core_date,
        first_timestamp AS core_datetime
    FROM
        marketing.stg_user__membership__merged
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
        userid,
        first_date AS ffa_date,
        first_timestamp AS ffa_datetime,
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
        product_su = 'lite'
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
lite_to_ffa AS (
    SELECT
        lite_membership.userid as lite_user,
        lite_membership.source,
        lite_membership.lite_date,
        core_membership.core_date,
        ffa_membership.ffa_date,
        ffa_membership.userid as ffa_user,
        coalesce(lite_membership.source_app,ffa_membership.source_app) as app_platform,
        ------ transformation ------
        case when install_campaign_user.userid is not null then 1 else 0 end is_install_campaign
    FROM
        lite_membership      
        LEFT JOIN core_membership ON lite_membership.userid = core_membership.userid
        LEFT JOIN ffa_membership ON lite_membership.userid = ffa_membership.userid
        LEFT JOIN install_campaign_user ON lite_membership.userid = install_campaign_user.userid
   where lower(app_platform)='android'
),
------ calculation ------
result AS (
    SELECT
        [lite_date:aggregation] dt,
        count(distinct lite_user) lite_count,
        count(core_date) core_count,
        count(distinct ffa_user) ffa_count
    FROM
        lite_to_ffa
    WHERE
        source = 'mobileApp'
        and is_install_campaign = 0
    GROUP BY
        1
),
final AS (
    SELECT
        dt,
        lite_count lite,
        ffa_count,
        (1.00 * ffa_count / lite_count) lite_to_ffa
    FROM
        result
)
SELECT
    *
FROM
    final
ORDER BY
    1 DESC