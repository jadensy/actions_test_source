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
        [first_open_date:aggregation] dt,
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
        (1.00 * lite_count / firstopen_count) lite_ffa
    FROM
        raw_data
)
SELECT
    *
FROM
    final
ORDER BY
    1 DESC