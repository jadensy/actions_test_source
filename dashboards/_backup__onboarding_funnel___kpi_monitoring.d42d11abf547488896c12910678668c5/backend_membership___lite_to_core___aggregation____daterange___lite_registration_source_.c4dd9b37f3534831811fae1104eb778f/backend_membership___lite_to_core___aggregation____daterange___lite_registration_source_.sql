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
-- lite to core
--------------------------------
lite_to_core AS (
    SELECT
        lite_membership.userid,
        lite_membership.source,
        lite_membership.lite_date,
        core_membership.core_date
    FROM
        lite_membership
        LEFT JOIN core_membership ON lite_membership.userid = core_membership.userid
),
result AS (
    SELECT
        lite_date AS dt,
        source AS lite_source,
        count(lite_date) lite_count,
        count(core_date) core_count
    FROM
        lite_to_core
    GROUP BY
        1,
        2
),
final AS (
    SELECT
        dt,
        lite_source,
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