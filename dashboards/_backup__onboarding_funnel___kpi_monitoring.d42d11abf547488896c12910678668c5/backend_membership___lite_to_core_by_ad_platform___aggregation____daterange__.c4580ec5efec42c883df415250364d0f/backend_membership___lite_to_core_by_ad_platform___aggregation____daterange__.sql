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
        userid,
        email,
        source,
        source_breakdown,
        first_date AS lite_date,
        first_timestamp AS lite_datetime
    FROM
        marketing.stg_user__membership__merged
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
-- attribution model
--------------------------------
product_last_touch AS (
    SELECT
        email,
        userid,
        source,
        campaign_type,
        ad_platform,
        touchtime,
        priority_rank,
        product_su,
        product_su_date
    FROM
        marketing.fct_attribution_model__product_last_touch
        CROSS JOIN date_select
    WHERE
        product_su_date >= date_select.start_date
        AND product_su_date <= date_select.end_date
        AND product_su = 'lite'
),
--------------------------------
-- lite to core
--------------------------------
lite_to_core AS (
    SELECT
        lite_membership.userid,
        lite_membership.source AS lite_source,
        lite_membership.lite_date,
        core_membership.core_date,
        COALESCE(product_last_touch.source, 'organic') lite_last_touch_source,
        COALESCE(product_last_touch.ad_platform, 'organic') ad_platform
    FROM
        lite_membership
        LEFT JOIN core_membership ON lite_membership.userid = core_membership.userid
        LEFT JOIN product_last_touch ON lite_membership.userid = product_last_touch.userid
),
result AS (
    SELECT
        lite_date AS dt,
        ad_platform,
        count(lite_date) lite_count,
        count(core_date) core_count
    FROM
        lite_to_core
    WHERE
        lite_last_touch_source = 'performance marketing'
    GROUP BY
        1,
        2
),
final AS (
    SELECT
        dt,
        ad_platform,
        lite_count a_lite,
        core_count b_core,
        (1.00 * core_count / lite_count) core_pct
    FROM
        result
)
SELECT
    *
FROM
    final
ORDER BY
    1 DESC