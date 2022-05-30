--no cache
WITH date_select AS (
    SELECT
        [daterange_start] start_date,
        [daterange_end] end_date
),
platform_select AS (
    SELECT
        'ios' :: varchar platform_select
),
------ ANONYMOUS ID USER EMAIL ------
event_user_email AS (
    SELECT
        *
    FROM
        (
            SELECT
                anonymous_id,
                user_id,
                ROW_NUMBER() OVER(
                    PARTITION BY anonymous_id
                    ORDER BY
                        "timestamp"
                ) rn_screen_viewed
            FROM
                marketing.fct_screen_viewed
            WHERE
                user_id IS NOT NULL
        ) stg_event_user_email
    WHERE
        rn_screen_viewed = 1
),
------ APPLICATION INSTALLED ------
android_first_open AS (
    SELECT
        anonymous_id,
        user_id AS user_email,
        "timestamp",
        platform
    FROM
        android.first_open
        CROSS JOIN date_select
        CROSS JOIN platform_select
    WHERE
        "timestamp" :: date >= date_select.start_date
        AND "timestamp" :: date <= date_select.end_date
        AND platform = platform_select.platform_select
),
ios_first_open AS (
    SELECT
        anonymous_id,
        user_id AS user_email,
        "timestamp",
        platform
    FROM
        ios.first_open
        CROSS JOIN date_select
        CROSS JOIN platform_select
    WHERE
        "timestamp" :: date >= date_select.start_date
        AND "timestamp" :: date <= date_select.end_date
        AND platform = platform_select.platform_select
),
prod_first_open AS (
    SELECT
        anonymous_id,
        user_id AS user_email,
        "timestamp",
        platform
    FROM
        prod.first_open
        CROSS JOIN date_select
        CROSS JOIN platform_select
    WHERE
        "timestamp" :: date >= date_select.start_date
        AND "timestamp" :: date <= date_select.end_date
        AND platform = platform_select.platform_select
),
stg_first_open AS (
    SELECT
        anonymous_id,
        user_email,
        platform,
        MIN("timestamp") :: timestamp AS first_open_timestamp,
        MIN("timestamp") :: date AS first_open_date
    FROM
        (
            SELECT
                *
            FROM
                android_first_open
            UNION
            ALL
            SELECT
                *
            FROM
                ios_first_open
            UNION
            ALL
            SELECT
                *
            FROM
                prod_first_open
        ) combined_first_open
    GROUP BY
        1,
        2,
        3
),
first_open AS (
    SELECT
        stg_first_open.anonymous_id,
        COALESCE(
            stg_first_open.user_email,
            event_user_email.user_id
        ) user_email,
        stg_first_open.platform,
        stg_first_open.first_open_timestamp,
        stg_first_open.first_open_date
    FROM
        stg_first_open
        LEFT JOIN event_user_email ON stg_first_open.anonymous_id = event_user_email.anonymous_id
),
------ LITE MEMBERSHIP ------
lite_membership AS (
    SELECT
        userid AS ml_user_id,
        email AS user_email,
        first_date :: date AS first_date,
        first_timestamp :: timestamp AS first_timestamp,
        product,
        rn
    FROM
        marketing.stg_user__membership__merged
    WHERE
        product = 'lite'
),
------ INSTALL TO LITE ------
install_to_lite AS (
    SELECT
        first_open.anonymous_id,
        first_open.user_email,
        first_open.platform,
        first_open.first_open_date,
        first_open.first_open_timestamp,
        lite_membership.first_date AS lite_date,
        lite_membership.first_timestamp AS lite_timestamp,
        datediff(DAY, first_open_date, lite_date) diff_date,
        (lite_timestamp - first_open_timestamp) diff_timestamp
    FROM
        first_open
        LEFT JOIN lite_membership ON first_open.user_email = lite_membership.user_email
),
raw_data AS (
    SELECT
        [first_open_date:aggregation] dt,
        COUNT(
            CASE
                WHEN first_open_date IS NOT NULL THEN 1
            END
        ) first_open_count,
        COUNT(
            CASE
                WHEN lite_date IS NOT NULL THEN 1
            END
        ) lite_count,
        COUNT(
            CASE
                WHEN diff_date < 1 THEN 1
            END
        ) d0_count,
        COUNT(
            CASE
                WHEN diff_date <= 1 THEN 1
            END
        ) d1_count,
        COUNT(
            CASE
                WHEN diff_date <= 7 THEN 1
            END
        ) d7_count,
        COUNT(
            CASE
                WHEN diff_date <= 14 THEN 1
            END
        ) d14_count,
        COUNT(
            CASE
                WHEN diff_date <= 30 THEN 1
            END
        ) d30_count,
        COUNT(
            CASE
                WHEN diff_date <= 60 THEN 1
            END
        ) d60_count,
        COUNT(
            CASE
                WHEN diff_date <= 90 THEN 1
            END
        ) d90_count
    FROM
        install_to_lite
    GROUP BY
        1
),
final AS (
    SELECT
        dt,
        first_open_count,
        lite_count,
        CASE
            WHEN date_select.end_date < dt THEN NULL
            ELSE 1.00 * d0_count / first_open_count
        END AS d0,
        CASE
            WHEN date_select.end_date < dt + 1 THEN NULL
            ELSE 1.00 * d1_count / first_open_count
        END AS d1,
        CASE
            WHEN date_select.end_date < dt + 7 THEN NULL
            ELSE 1.00 * d7_count / first_open_count
        END AS d7,
        CASE
            WHEN date_select.end_date < dt + 14 THEN NULL
            ELSE 1.00 * d14_count / first_open_count
        END AS d14,
        CASE
            WHEN date_select.end_date < dt + 30 THEN NULL
            ELSE 1.00 * d30_count / first_open_count
        END AS d30,
        CASE
            WHEN date_select.end_date < dt + 60 THEN NULL
            ELSE 1.00 * d60_count / first_open_count
        END AS d60,
        CASE
            WHEN date_select.end_date < dt + 90 THEN NULL
            ELSE 1.00 * d90_count / first_open_count
        END AS d90,
        CASE
            WHEN date_select.end_date > dt + 90 THEN 1.00 * lite_count / first_open_count
        END AS d90
    FROM
        raw_data
        CROSS JOIN date_select
)
SELECT
    *
FROM
    final
ORDER BY
    1 DESC