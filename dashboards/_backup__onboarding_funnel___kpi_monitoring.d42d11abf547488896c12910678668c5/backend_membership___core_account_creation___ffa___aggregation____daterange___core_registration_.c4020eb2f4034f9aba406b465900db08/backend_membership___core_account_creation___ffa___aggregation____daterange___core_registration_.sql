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
-- core to ffa
--------------------------------
core_to_ffa AS (
    SELECT
        core_membership.userid,
        core_membership.source,
        core_membership.core_date,
        ffa_membership.ffa_date
    FROM
        core_membership
        LEFT JOIN ffa_membership ON core_membership.userid = ffa_membership.userid
),
result AS (
    SELECT
        core_date AS dt,
        source,
        count(core_date) core_count,
        count(ffa_date) ffa_count
    FROM
        core_to_ffa
    GROUP BY
        1,
        2
),
final AS (
    SELECT
        dt,
        source,
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