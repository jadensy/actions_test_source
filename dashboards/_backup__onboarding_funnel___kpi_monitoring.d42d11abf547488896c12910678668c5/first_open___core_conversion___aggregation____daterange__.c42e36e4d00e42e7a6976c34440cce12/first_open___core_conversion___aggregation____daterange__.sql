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
        [first_open_date:aggregation] dt,
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
    GROUP BY
        1
),
final AS (
    SELECT
        dt,
        firstopen_count AS firstopen,
        (1.00 * landing_count / firstopen_count) landing,
        (1.00 * firstname_count / firstopen_count) firstname,
        (1.00 * email_count / firstopen_count) email,
        (1.00 * password_count / firstopen_count) pwd,
        (1.00 * lite_event_count / firstopen_count) lite_event,
        (1.00 * passcode_count / firstopen_count) passcode,
        (1.00 * lite_count / firstopen_count) lite_ffa,
        (1.00 * core_intro_count / firstopen_count) core_intro,
        (1.00 * core_dob_count / firstopen_count) core_dob,
        (1.00 * core_address_count / firstopen_count) core_address,
        (1.00 * core_phone_count / firstopen_count) core_phone,
        (1.00 * core_ssn_count / firstopen_count) core_ssn,
        (1.00 * core_id_count / firstopen_count) core_id,
        (1.00 * core_event_count / firstopen_count) core_event,
        (1.00 * core_count / firstopen_count) core_ffa
    FROM
        raw_data
)
SELECT
    *
FROM
    final
ORDER BY
    1 DESC