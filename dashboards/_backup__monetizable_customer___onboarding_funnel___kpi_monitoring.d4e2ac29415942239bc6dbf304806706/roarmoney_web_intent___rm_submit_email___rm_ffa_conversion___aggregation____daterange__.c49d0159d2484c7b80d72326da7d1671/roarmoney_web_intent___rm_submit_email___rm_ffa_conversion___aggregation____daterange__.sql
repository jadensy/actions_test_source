WITH date_select AS (
    SELECT
        [daterange_start] start_date,
        [daterange_end] end_date
),
user_anon_session AS (
    SELECT
        page_loaded.timestamp :: date grass_date,
        page_loaded.timestamp,
        -- page_loaded.user_id,
        page_loaded.anonymous_id,
        ROW_NUMBER() OVER(
            PARTITION BY page_loaded.user_id,
            page_loaded.anonymous_id
            ORDER BY
                page_loaded.timestamp ASC
        ) rn
    FROM
        onboarding_web.page_loaded
        CROSS JOIN date_select
    WHERE
        page_loaded.timestamp :: date >= date_select.start_date
        AND page_loaded.timestamp :: date <= date_select.end_date
        AND LOWER(page_loaded.page_path) = 'roarmoney_submit_email'
),
user_ffa_session AS (
    SELECT
        anonymous_id,
        user_id email,
        timestamp :: date ffa_checkpoint_date
    FROM
        onboarding_web.dda_account_created
        CROSS JOIN date_select
    WHERE
        timestamp :: date >= date_select.start_date
),
rm_ffa_membership AS (
    SELECT
        userid,
        email,
        first_date,
        first_timestamp
    FROM
        marketing.stg_user__membership__merged membership
    WHERE
        product = 'roar money'
),
------ MEMBERSHIP SIGN UP DATA ------
membership_su AS (
    SELECT
        user_anon_session.anonymous_id,
        user_anon_session.grass_date rm_submit_email_date,
        rm_ffa_membership.first_date rm_ffa_date
    FROM
        user_anon_session
        LEFT JOIN user_ffa_session ON (
            user_anon_session.anonymous_id = user_ffa_session.anonymous_id
            AND user_anon_session.rn = 1
        )
        LEFT JOIN rm_ffa_membership ON user_ffa_session.email = rm_ffa_membership.email
),
membership_su_data AS (
    SELECT
        [rm_submit_email_date:aggregation] rm_submit_email_date,
        COUNT(
            CASE
                WHEN rm_submit_email_date IS NOT NULL THEN 1
            END
        ) submit_email_count,
        COUNT(
            CASE
                WHEN rm_ffa_date IS NOT NULL THEN 1
            END
        ) rm_ffa_count,
        COUNT(
            CASE
                WHEN rm_ffa_date - rm_submit_email_date < 1 THEN 1
            END
        ) d0_count,
        COUNT(
            CASE
                WHEN rm_ffa_date - rm_submit_email_date <= 1 THEN 1
            END
        ) d1_count,
        COUNT(
            CASE
                WHEN rm_ffa_date - rm_submit_email_date <= 7 THEN 1
            END
        ) d7_count,
        COUNT(
            CASE
                WHEN rm_ffa_date - rm_submit_email_date <= 14 THEN 1
            END
        ) d14_count,
        COUNT(
            CASE
                WHEN rm_ffa_date - rm_submit_email_date <= 30 THEN 1
            END
        ) d30_count,
        COUNT(
            CASE
                WHEN rm_ffa_date - rm_submit_email_date <= 60 THEN 1
            END
        ) d60_count,
        COUNT(
            CASE
                WHEN rm_ffa_date - rm_submit_email_date <= 90 THEN 1
            END
        ) d90_count
    FROM
        membership_su
    GROUP BY
        1
),
------ RAW DATA ------
raw_data AS (
    SELECT
        rm_submit_email_date AS dt,
        submit_email_count AS rm_submit_email,
        rm_ffa_count AS rm_ffa,
        1.00 * d0_count / submit_email_count AS d0,
        CASE
            WHEN date_select.end_date < rm_submit_email_date + 1 THEN NULL
            ELSE 1.00 * d1_count / submit_email_count
        END d1,
        CASE
            WHEN date_select.end_date < rm_submit_email_date + 7 THEN NULL
            ELSE 1.00 * d7_count / submit_email_count
        END d7,
        CASE
            WHEN date_select.end_date < rm_submit_email_date + 14 THEN NULL
            ELSE 1.00 * d14_count / submit_email_count
        END d14,
        CASE
            WHEN date_select.end_date < rm_submit_email_date + 30 THEN NULL
            ELSE 1.00 * d30_count / submit_email_count
        END d30,
        CASE
            WHEN date_select.end_date < rm_submit_email_date + 60 THEN NULL
            ELSE 1.00 * d60_count / submit_email_count
        END d60,
        CASE
            WHEN date_select.end_date < rm_submit_email_date + 90 THEN NULL
            ELSE 1.00 * d90_count / submit_email_count
        END d90,
        CASE
            WHEN date_select.end_date > rm_submit_email_date + 90 THEN 1.00 * rm_ffa_count / submit_email_count
            ELSE NULL
        END "d90+"
    FROM
        membership_su_data
        CROSS JOIN date_select
    ORDER BY
        1 DESC
)
SELECT
    *
FROM
    raw_data