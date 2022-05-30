WITH date_select AS (
    SELECT
        [daterange_start] start_date,
        [daterange_end] end_date
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
        first_timestamp AS ffa_datetimte
    FROM
        marketing.stg_user__membership__merged
    WHERE
        product NOT IN ('lite', 'new core')
        AND rn = 1
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
        AND product_su = 'new core'
),
--------------------------------
-- core to ffa
--------------------------------
core_to_ffa AS (
    SELECT
        core_membership.userid,
        core_membership.source AS core_source,
        core_membership.core_date,
        ffa_membership.ffa_date,
        COALESCE(product_last_touch.source, 'organic') source,
        COALESCE(product_last_touch.ad_platform, 'organic') ad_platform
    FROM
        core_membership
        LEFT JOIN ffa_membership ON core_membership.userid = ffa_membership.userid
        LEFT JOIN product_last_touch ON core_membership.userid = product_last_touch.userid
),
result AS (
    SELECT
        core_date AS dt,
        ad_platform,
        count(core_date) core_count,
        count(ffa_date) ffa_count
    FROM
        core_to_ffa
    WHERE
        core_source = 'mobileApp'
        AND source = 'performance marketing'
    GROUP BY
        1,
        2
),
final AS (
    SELECT
        dt,
        ad_platform,
        core_count,
        ffa_count,
        (1.00 * ffa_count / core_count) ffa_pct
    FROM
        result
)
SELECT
    *
FROM
    final
ORDER BY
    1 DESC