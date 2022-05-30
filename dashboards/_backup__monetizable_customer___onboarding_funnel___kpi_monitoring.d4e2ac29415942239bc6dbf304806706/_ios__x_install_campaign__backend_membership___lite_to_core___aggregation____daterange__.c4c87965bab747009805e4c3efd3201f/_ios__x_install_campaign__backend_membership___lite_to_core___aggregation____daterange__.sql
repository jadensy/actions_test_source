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
        a.first_date AS core_date,
        a.first_timestamp AS core_datetime,
        a.source as core_source,
        case when a.source_breakdown='uncategorized' then b.platform else a.source_breakdown end as source_app
    FROM
        marketing.stg_user__membership__merged a
        left join marketing.fct_install_attribution_model__install_last_touch b on a.email=b.user_email and a.first_timestamp=b.lite_timestamp and a.product=b.lite
        CROSS JOIN date_select
    WHERE
        product = 'new core'
        AND first_date >= date_select.start_date
        AND first_date <= date_select.end_date
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
-- lite to core
--------------------------------
lite_to_core AS (
    SELECT
        lite_membership.userid as lite_user,
        core_membership.userid as core_user,
        lite_membership.source,
        lite_membership.lite_date,
        core_membership.core_date,
        coalesce(lite_membership.source_app, core_membership.source_app) as app_breakdown,
        ------ transformation ------
        CASE
            WHEN install_campaign_user.userid IS NOT NULL THEN 1
            ELSE 0
        END is_install_campaign
    FROM
        lite_membership
        LEFT JOIN core_membership ON lite_membership.userid = core_membership.userid
        LEFT JOIN install_campaign_user ON lite_membership.userid = install_campaign_user.userid
    WHERE
        lite_membership.source = 'mobileApp'
        AND is_install_campaign = 0
        AND lower(app_breakdown)='ios'
),


result AS (
    SELECT
        [lite_date:aggregation] AS dt,
        count(distinct lite_user) lite_count,
        count(distinct core_user) core_count
    FROM
        lite_to_core
    GROUP BY
        1
),
final AS (
    SELECT
        dt,
        lite_count lite,
        core_count core,
        (1.00 * core_count / lite_count) lite_to_core
    FROM
        result
)
SELECT
    *
FROM
    final
ORDER BY
    1 DESC