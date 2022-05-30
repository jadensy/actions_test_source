WITH date_select AS (
    SELECT
        [daterange_start] start_date,
        [daterange_end] end_date
),
--------------------------------
-- registered users
--------------------------------
uscol_users AS (
    SELECT
        userid,
        email,
        first_date AS uscol_created_date,
        first_timestamp AS reg_time
    FROM
        marketing.stg_user__membership__merged a
        CROSS JOIN date_select
    WHERE
        TRUE
        AND a.product = 'lite'
        AND first_date >= date_select.start_date
        AND first_date <= date_select.end_date
),
--------------------------------
-- core users
--------------------------------
new_core_user AS (
    SELECT
        userid,
        first_date,
        first_timestamp,
        product
    FROM
        marketing.stg_user__membership__merged a
        CROSS JOIN date_select
    WHERE
        TRUE
        AND a.product = 'new core'
        AND first_date >= date_select.start_date
        AND first_date <= date_select.end_date
),
--------------------------------
-- Joins
--------------------------------
combine_reg_core_user AS (
    SELECT
        a.userid,
        a.reg_time AS reg_date,
        b.first_timestamp - a.reg_time AS reg_core_int_time,
        b.first_date AS core_su_date
    FROM
        uscol_users a
        LEFT JOIN new_core_user b ON a.userid = b.userid
),
combine_reg_core_user_cum AS (
    SELECT
        [reg_date:aggregation] AS reg_dt,
        count(
            CASE
                WHEN reg_date IS NOT NULL THEN 1
                ELSE NULL
            END
        ) AS reg_count,
        count(
            CASE
                WHEN core_su_date IS NOT NULL THEN 1
                ELSE NULL
            END
        ) AS core_count,
        count(
            CASE
                WHEN core_su_date IS NULL THEN 1
                ELSE NULL
            END
        ) AS no_core_count
    FROM
        combine_reg_core_user
    GROUP BY
        1
),
final AS (
    SELECT
        reg_dt,
        reg_count,
        core_count,
        (1.00 * core_count / reg_count) core_pct
    FROM
        combine_reg_core_user_cum
)
SELECT
    *
FROM
    final
ORDER BY
    1 DESC