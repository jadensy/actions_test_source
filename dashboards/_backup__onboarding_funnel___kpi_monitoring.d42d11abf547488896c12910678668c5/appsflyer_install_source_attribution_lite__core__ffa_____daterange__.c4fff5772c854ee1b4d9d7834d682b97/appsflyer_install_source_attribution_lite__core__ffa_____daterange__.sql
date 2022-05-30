WITH date_select AS (
    SELECT
        [daterange_start] start_date,
        [daterange_end] end_date
),
------ install attribution ------
install_model AS (
    SELECT
        appsflyer_id,
        platform,
        event_time,
        touch_sequence,
        source,
        ad_platform,
        app_priority_rank,
        install_date,
        install_timestamp,
        ml_user_id,
        user_email,
        lite_date,
        lite_timestamp,
        lite_source,
        core_date,
        core_timestamp,
        core_source,
        ffa_date,
        ffa_timestamp,
        rn_ffa,
        ffa_product,
        ffa_source,
        lite_register
    FROM
        marketing.fct_install_attribution_model__install_last_touch
        CROSS JOIN date_select
    WHERE
        install_date >= date_select.start_date
        AND install_date <= date_select.end_date
        AND (
            rn_ffa IS NULL
            OR rn_ffa = 1
        )
        AND touch_sequence IN ('last touch', 'organic')
),
------ calculation ------
final AS (
    SELECT
        source,
        count(appsflyer_id) install,
        count(
            CASE
                WHEN lite_register IN ('no_lite', 'mobileApp') THEN appsflyer_id
            END
        ) normal_install,
        count(
            CASE
                WHEN lite_register NOT IN ('no_lite', 'mobileApp') THEN appsflyer_id
            END
        ) pre_signup,
        count(
            CASE
                WHEN lite_register = 'webApp' THEN appsflyer_id
            END
        ) web_signup,
        count(
            CASE
                WHEN lite_register = 'previous_signup' THEN appsflyer_id
            END
        ) previous_signup
    FROM
        install_model
    GROUP BY
        1
)
SELECT
    source,
    install,
    (1.00 * normal_install / install) normal_install_pct,
    (1.00 * web_signup / install) web_signup_pct,
    (1.00 * previous_signup / install) previous_signup_pct
FROM
    final
ORDER BY
    2 DESC