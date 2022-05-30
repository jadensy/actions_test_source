WITH date_select AS (
    SELECT
        [daterange_start] start_date,
        ([daterange_end] - INTERVAL '2 day') :: date end_date
),
--------------------------------
-- USER JOURNEY
--------------------------------
------ ONBOARDING JOURNEY ------
stg_onboarding_funnel__first_open AS (
    SELECT
        anonymous_id,
        first_open_date,
        first_open_timestamp,
        rn_first_open
    FROM
        marketing.stg_onboarding_funnel__first_open
        CROSS JOIN date_select
    WHERE
        first_open_date >= date_select.start_date
        AND first_open_date <= date_select.end_date
),
stg_onboarding_funnel__lite_checkpoint_journey AS (
    SELECT
        anonymous_id,
        landing_date,
        first_name_date,
        email_date,
        password_date,
        passcode_date,
        lite_passcode_email
    FROM
        marketing.stg_onboarding_funnel__lite_checkpoint_journey
        CROSS JOIN date_select
    WHERE
        landing_date >= date_select.start_date
        AND landing_date <= date_select.end_date
),
stg_onboarding_funnel__lite_account_created AS (
    SELECT
        anonymous_id,
        user_id,
        lite_event_date,
        rn_lite_event
    FROM
        marketing.stg_onboarding_funnel__lite_account_created
        CROSS JOIN date_select
    WHERE
        lite_event_date >= date_select.start_date
        AND lite_event_date <= date_select.end_date
),
lite_membership AS (
    SELECT
        userid,
        email,
        first_date AS lite_date
    FROM
        marketing.stg_user__membership__merged
    WHERE
        product = 'lite'
),
onboarding_journey AS (
    SELECT
        first_open.anonymous_id,
        first_open.first_open_date,
        lite_checkpoint_journey.landing_date,
        lite_checkpoint_journey.first_name_date,
        lite_checkpoint_journey.email_date,
        lite_checkpoint_journey.password_date,
        lite_checkpoint_journey.passcode_date,
        lite_checkpoint_journey.lite_passcode_email,
        lite_account_created.lite_event_date,
        lite_membership.lite_date
    FROM
        stg_onboarding_funnel__first_open AS first_open
        LEFT JOIN stg_onboarding_funnel__lite_checkpoint_journey AS lite_checkpoint_journey ON (
            first_open.anonymous_id = lite_checkpoint_journey.anonymous_id
            AND first_open.first_open_date = lite_checkpoint_journey.landing_date
        )
        LEFT JOIN stg_onboarding_funnel__lite_account_created AS lite_account_created ON (
            lite_checkpoint_journey.anonymous_id = lite_account_created.anonymous_id
            AND datediff(
                HOUR,
                lite_checkpoint_journey.landing_date,
                lite_account_created.lite_event_date
            ) <= 24
        )
        LEFT JOIN lite_membership ON (
            lite_checkpoint_journey.lite_passcode_email = lite_membership.email
        )
),
--------------------------------
-- RAW DATA
--------------------------------
raw_data AS (
    SELECT
        count(first_open_date) firstopen_count,
        count(landing_date) landing_count,
        count(first_name_date) firstname_count,
        count(email_date) email_count,
        count(password_date) password_count,
        count(lite_event_date) lite_event_count,
        count(passcode_date) passcode_count,
        count(lite_date) lite_count
    FROM
        onboarding_journey
),
stg_final AS (
    SELECT
        1 AS order_key,
        'firstopen' AS funnel_desc,
        firstopen_count AS event_count
    FROM
        raw_data
    UNION
    ALL
    SELECT
        2 AS order_key,
        'landing' AS funnel_desc,
        landing_count AS event_count
    FROM
        raw_data
    UNION
    ALL
    SELECT
        3 AS order_key,
        'firstname' AS funnel_desc,
        firstname_count AS event_count
    FROM
        raw_data
    UNION
    ALL
    SELECT
        4 AS order_key,
        'email' AS funnel_desc,
        email_count AS event_count
    FROM
        raw_data
    UNION
    ALL
    SELECT
        5 AS order_key,
        'pwd' AS funnel_desc,
        password_count AS event_count
    FROM
        raw_data
    UNION
    ALL
    SELECT
        6 AS order_key,
        'lite_event' AS funnel_desc,
        lite_event_count AS event_count
    FROM
        raw_data
    UNION
    ALL
    SELECT
        7 AS order_key,
        'passcode' AS funnel_desc,
        passcode_count AS event_count
    FROM
        raw_data
    UNION
    ALL
    SELECT
        8 AS order_key,
        'lite' AS funnel_desc,
        lite_count AS event_count
    FROM
        raw_data
),
final AS (
    SELECT
        t1.order_key,
        t1.funnel_desc,
        t1.event_count,
        (1.00 * t1.event_count / t2.firstopen_count) conversion_pct,
        (1.00 * t1.event_count / t1.previous_event_count) previous_step_pct
    FROM
        (
            SELECT
                order_key,
                funnel_desc,
                event_count,
                lag(event_count, 1) over(
                    ORDER BY
                        order_key ASC
                ) previous_event_count
            FROM
                stg_final
        ) t1
        CROSS JOIN (
            SELECT
                firstopen_count
            FROM
                raw_data
        ) t2
    ORDER BY
        t1.order_key ASC
)
SELECT
    *
FROM
    final