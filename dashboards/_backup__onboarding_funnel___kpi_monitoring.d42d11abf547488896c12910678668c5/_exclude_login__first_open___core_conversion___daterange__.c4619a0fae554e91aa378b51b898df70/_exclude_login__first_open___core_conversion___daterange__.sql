WITH date_select AS (
    SELECT
        [daterange_start] start_date,
        ([daterange_end] - INTERVAL '2 day') :: date end_date
),
--------------------------------
-- ONBOARDING JOURNEY
--------------------------------
onboarding_journey_exclude_login AS (
    SELECT
        anonymous_id,
        first_open_date,
        landing_date,
        first_name_date,
        email_date,
        password_date,
        passcode_date,
        lite_passcode_email,
        lite_event_date,
        lite_date,
        core_intro_date,
        core_dob_date,
        core_address_date,
        core_phone_date,
        core_ssn_date,
        core_id_date,
        core_event_date,
        core_date
    FROM
        marketing.fct_onboarding_funnel__onboarding_journey_exclude_login
        CROSS JOIN date_select
    WHERE
        first_open_date >= date_select.start_date
        AND first_open_date <= date_select.end_date
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
        count(lite_date) lite_count,
        COUNT(core_intro_date) core_intro_count,
        COUNT(core_dob_date) core_dob_count,
        COUNT(core_address_date) core_address_count,
        COUNT(core_phone_date) core_phone_count,
        COUNT(core_ssn_date) core_ssn_count,
        COUNT(core_id_date) core_id_count,
        COUNT(core_event_date) core_event_count,
        COUNT(core_date) core_count
    FROM
        onboarding_journey_exclude_login
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
    UNION
    ALL
    SELECT
        9 AS order_key,
        'core_intro' AS funnel_desc,
        core_intro_count AS event_count
    FROM
        raw_data
    UNION
    ALL
    SELECT
        10 AS order_key,
        'core_dob' AS funnel_desc,
        core_dob_count AS event_count
    FROM
        raw_data
    UNION
    ALL
    SELECT
        11 AS order_key,
        'core_address' AS funnel_desc,
        core_address_count AS event_count
    FROM
        raw_data
    UNION
    ALL
    SELECT
        12 AS order_key,
        'core_phone' AS funnel_desc,
        core_phone_count AS event_count
    FROM
        raw_data
    UNION
    ALL
    SELECT
        13 AS order_key,
        'core_ssn' AS funnel_desc,
        core_ssn_count AS event_count
    FROM
        raw_data
    UNION
    ALL
    SELECT
        14 AS order_key,
        'core_id' AS funnel_desc,
        core_id_count AS event_count
    FROM
        raw_data
    UNION
    ALL
    SELECT
        15 AS order_key,
        'core_event' AS funnel_desc,
        core_event_count AS event_count
    FROM
        raw_data
    UNION
    ALL
    SELECT
        16 AS order_key,
        'core_date' AS funnel_desc,
        core_count AS event_count
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