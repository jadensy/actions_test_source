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
        onboarding_journey_exclude_login
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