--no cache
WITH date_select AS (
    SELECT
        [daterange_start] start_date,
        [daterange_end] end_date
),
------ APPLICATION INSTALL ------
install AS (
    SELECT
        DISTINCT a."timestamp" AS install_time,
        a.context_os_name AS os_type,
        coalesce(a.user_id, b.user_id) AS email
    FROM
        prod.application_installed a
        LEFT JOIN (
            SELECT
                DISTINCT anonymous_id,
                user_id
            FROM
                prod.screen_viewed
        ) b ON a.anonymous_id = b.anonymous_id
        CROSS JOIN date_select
    WHERE
        a.timestamp :: date >= date_select.start_date
        AND a.timestamp :: date <= date_select.end_date
    UNION
    ALL
    SELECT
        DISTINCT a."timestamp" AS install_time,
        a.context_os_name AS os_type,
        coalesce(a.user_id, b.user_id) AS email
    FROM
        ios.application_installed a
        LEFT JOIN (
            SELECT
                DISTINCT anonymous_id,
                user_id
            FROM
                ios.screen_viewed
        ) b ON a.anonymous_id = b.anonymous_id
        CROSS JOIN date_select
    WHERE
        a.timestamp :: date >= date_select.start_date
        AND a.timestamp :: date <= date_select.end_date
    UNION
    ALL
    SELECT
        DISTINCT a."timestamp" AS install_time,
        a.context_os_name AS os_type,
        b.user_id AS email
    FROM
        android.application_installed a
        LEFT JOIN (
            SELECT
                DISTINCT anonymous_id,
                user_id
            FROM
                android.screen_viewed
        ) b ON a.anonymous_id = b.anonymous_id
        CROSS JOIN date_select
    WHERE
        a.timestamp :: date >= date_select.start_date
        AND a.timestamp :: date <= date_select.end_date
),
install_users AS (
    SELECT
        email,
        min(date(install_time)) AS first_install_date
    FROM
        install
    GROUP BY
        1
    HAVING
        min(date(install_time)) IS NOT NULL
    ORDER BY
        1
),
--website su--
web_su AS (
    SELECT
        dt,
        dtime,
        email,
        platform
    FROM
        [web_lite_user]
),
/*  select
 lower(user_id) as email,
 min("timestamp") as min_tracked
 from onboarding_web.page_loaded as a
 where user_id is not null
 group by 1
 
 ),
 
 web_su as (
 
 select
 date(min_tracked) as dt,
 min_tracked as dtime,
 a.email,
 'web' as platform    
 from web_min_date as a
 join lion1.user as b
 on b.email = a.email
 and date(b.createdon) = date(a.min_tracked)
 --       and b.createdon <= a.min_tracked + interval '20 minutes'
 where true
 and b.deleted <> 1
 and b.brand = 'ml'
 ),*/
app_su AS (
    SELECT
        dt,
        su_time AS dtime,
        email,
        platform
    FROM
        [app_lite_user]
),
/*select distinct
 a.dt,
 a.dtime,
 a.email,
 a.platform   
 from(
 select 
 date("timestamp") as dt,
 "timestamp" as dtime,
 lower(user_id) as email,
 'ios' as platform
 
 from ios.screen_viewed 
 where true
 and user_id is not null 
 
 union all
 
 select 
 date("timestamp") as dt,
 "timestamp" as dtime,
 lower(user_id) as email,
 'android' as platform
 
 from android.screen_viewed 
 where true
 and user_id is not null 
 )a
 join lion1.user as b
 on b.email = a.email
 and date(b.createdon) = a.dt
 --       and b.createdon <= a.dtime + interval '20 minutes'
 where true
 and b.deleted <> 1
 and b.brand = 'ml'
 ),*/
combined_su AS (
    SELECT
        dt,
        dtime,
        email,
        platform,
        row_number() over (
            PARTITION by email
            ORDER BY
                dtime ASC
        ) AS rn
    FROM
        (
            SELECT
                *
            FROM
                web_su
            UNION
            ALL
            SELECT
                *
            FROM
                app_su
        ) AS a
),
ffa_users AS (
    SELECT
        a.userid,
        a.email,
        a.product,
        a.first_date,
        a.first_timestamp,
        c.platform
    FROM
        marketing.stg_user__membership__merged AS a
        LEFT JOIN combined_su AS c ON c.email = a.email
        AND c.rn = 1
    WHERE
        TRUE
        AND a.product IN ('lite')
),
combine_ins_lite_user AS (
    SELECT
        DISTINCT a.email,
        b.first_install_date AS install_time,
        a.first_date AS lite_time,
        datediff(DAY, lite_time, install_time) AS days_to_sec_prod
    FROM
        ffa_users a
        LEFT JOIN install_users b ON a.email = b.email
    WHERE
        a.platform = 'web'
),
combine_ins_lite_user_cum AS (
    SELECT
        [lite_time:aggregation] AS lite_date,
        --date(date_trunc('month',lite_time)) as lite_month,
        count(
            CASE
                WHEN lite_time IS NOT NULL THEN 1
                ELSE NULL
            END
        ) AS lite_count,
        count(
            CASE
                WHEN install_time IS NOT NULL THEN 1
                ELSE NULL
            END
        ) AS ins_count,
        count(
            CASE
                WHEN install_time IS NULL THEN 1
                ELSE NULL
            END
        ) AS no_ins_count,
        count(
            CASE
                WHEN days_to_sec_prod < 1 THEN 1
                ELSE NULL
            END
        ) AS d0,
        count(
            CASE
                WHEN days_to_sec_prod <= 1 THEN 1
                ELSE NULL
            END
        ) AS d1,
        count(
            CASE
                WHEN days_to_sec_prod <= 7 THEN 1
                ELSE NULL
            END
        ) AS d7,
        count(
            CASE
                WHEN days_to_sec_prod <= 14 THEN 1
                ELSE NULL
            END
        ) AS d14,
        count(
            CASE
                WHEN days_to_sec_prod <= 30 THEN 1
                ELSE NULL
            END
        ) AS d30,
        count(
            CASE
                WHEN days_to_sec_prod <= 60 THEN 1
                ELSE NULL
            END
        ) AS d60,
        count(
            CASE
                WHEN days_to_sec_prod <= 90 THEN 1
                ELSE NULL
            END
        ) AS d90,
        count(
            CASE
                WHEN days_to_sec_prod IS NOT NULL THEN 1
                ELSE NULL
            END
        ) AS d90plus
    FROM
        combine_ins_lite_user
    WHERE
        lite_date >= '2020-01-01' --and (days_to_sec_prod is null or days_to_sec_prod >=0)
    GROUP BY
        lite_date
    ORDER BY
        lite_date DESC
)
SELECT
    lite_date,
    lite_count,
    ins_count,
    no_ins_count * 1.0 / lite_count AS "no_ins_%",
    CASE
        WHEN getdate() < lite_date THEN NULL
        ELSE d0 * 1.0 / lite_count
    END AS d0,
    CASE
        WHEN getdate() < lite_date + 1 THEN NULL
        ELSE d1 * 1.0 / lite_count
    END AS d1,
    CASE
        WHEN getdate() < lite_date + 7 THEN NULL
        ELSE d7 * 1.0 / lite_count
    END AS d7,
    CASE
        WHEN getdate() < lite_date + 14 THEN NULL
        ELSE d14 * 1.0 / lite_count
    END AS d14,
    CASE
        WHEN getdate() < lite_date + 30 THEN NULL
        ELSE d30 * 1.0 / lite_count
    END AS d30,
    CASE
        WHEN getdate() < lite_date + 60 THEN NULL
        ELSE d60 * 1.0 / lite_count
    END AS d60,
    CASE
        WHEN getdate() < lite_date + 90 THEN NULL
        ELSE d90 * 1.0 / lite_count
    END AS d90,
    CASE
        WHEN getdate() > lite_date + 90 THEN d90plus * 1.0 / lite_count
        ELSE NULL
    END AS "d90+"
FROM
    combine_ins_lite_user_cum