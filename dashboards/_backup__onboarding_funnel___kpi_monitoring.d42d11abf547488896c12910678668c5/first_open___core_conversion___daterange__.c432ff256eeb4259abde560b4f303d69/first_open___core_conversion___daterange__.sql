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
stg_onboarding_funnel__core_checkpoint_journey AS (
    SELECT
        anonymous_id,
        landing_date,
        core_intro_date,
        core_dob_date,
        core_address_date,
        core_phone_date,
        core_ssn_date,
        core_id_date,
        core_intro_email
    FROM
        marketing.stg_onboarding_funnel__core_checkpoint_journey
        CROSS JOIN date_select
    WHERE
        landing_date >= date_select.start_date
        AND landing_date <= date_select.end_date
),
stg_onboarding_funnel__core_membership_created AS (
    SELECT
        anonymous_id,
        user_id,
        core_event_date,
        rn_core_event
    FROM
        marketing.stg_onboarding_funnel__core_membership_created
        CROSS JOIN date_select
    WHERE
        core_event_date >= date_select.start_date
        AND core_event_date <= date_select.end_date
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
core_membership AS (
    SELECT
        userid,
        email,
        first_date AS core_date
    FROM
        marketing.stg_user__membership__merged
    WHERE
        product = 'new core'
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
        lite_membership.lite_date,
        core_checkpoint_journey.core_intro_date,
        core_checkpoint_journey.core_dob_date,
        core_checkpoint_journey.core_address_date,
        core_checkpoint_journey.core_phone_date,
        core_checkpoint_journey.core_ssn_date,
        core_checkpoint_journey.core_id_date,
        core_checkpoint_journey.core_intro_email,
        core_membership_created.core_event_date,
        core_membership.core_date
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
        LEFT JOIN stg_onboarding_funnel__core_checkpoint_journey AS core_checkpoint_journey ON (
            lite_checkpoint_journey.lite_passcode_email = core_checkpoint_journey.core_intro_email
            AND lite_checkpoint_journey.landing_date = core_checkpoint_journey.landing_date
        )
        LEFT JOIN stg_onboarding_funnel__core_membership_created AS core_membership_created ON (
            core_checkpoint_journey.core_intro_email = core_membership_created.user_id
            AND core_checkpoint_journey.core_id_date = core_membership_created.core_event_date
        )
        LEFT JOIN core_membership ON (
            core_membership_created.user_id = core_membership.email
        )
),
--------------------------------
-- RAW DATA
--------------------------------
raw_data AS (
    SELECT
        COUNT(first_open_date) firstopen_count,
        COUNT(landing_date) landing_count,
        COUNT(first_name_date) firstname_count,
        COUNT(email_date) email_count,
        COUNT(password_date) password_count,
        COUNT(lite_event_date) lite_event_count,
        COUNT(passcode_date) passcode_count,
        COUNT(lite_date) lite_count,
        COUNT(core_intro_date) core_intro_count,
        COUNT(core_dob_date) core_dob_count,
        COUNT(core_address_date) core_address_count,
        COUNT(core_phone_date) core_phone_count,
        COUNT(core_ssn_date) core_ssn_count,
        COUNT(core_id_date) core_id_count,
        COUNT(core_event_date) core_event_count,
        COUNT(core_date) core_count
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