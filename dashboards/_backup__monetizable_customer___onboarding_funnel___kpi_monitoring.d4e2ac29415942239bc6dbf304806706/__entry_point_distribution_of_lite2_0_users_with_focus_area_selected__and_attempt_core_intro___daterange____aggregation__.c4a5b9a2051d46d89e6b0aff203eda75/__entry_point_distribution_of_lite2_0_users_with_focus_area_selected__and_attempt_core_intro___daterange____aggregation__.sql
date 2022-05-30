/*
MAIN: LITE 2.0 JOURNEY ANALYSIS
*/

WITH date_select AS (
    SELECT
        '2022-01-25' :: date start_date,
        '2022-02-23' :: date end_date
),

gp_selection_focus_area as (
    select
        userid,
        email,
        lite_timestamp,
        screen_timestamp
    from
        [gp_selection_focus_area]
        cross join date_select
    where
        gp_user_id is not null
        and lite_timestamp :: date >= date_select.start_date
        and lite_timestamp :: date <= date_select.end_date
),

-- ==========================
-- path to product selection 
-- ==========================
------ core onboarding ------
core_intro as (
    select
        user_id,
        checkpoint_date,
        checkpoint_timestamp,
        flow,
        name 
    from 
        (
            select *
            from 
                marketing.stg_onboarding_funnel__onboarding_checkpoint_hit_ios
                cross join date_select
            where
                flow = 'Membership'
                and name = 'Core Onboarding Intro'
                and checkpoint_date >= date_select.start_date
                and checkpoint_date <= date_select.end_date
            union all
            select * 
            from 
                marketing.stg_onboarding_funnel__onboarding_checkpoint_hit_android
                cross join date_select
            where
                flow = 'Membership'
                and name = 'Core Onboarding Intro'
                and checkpoint_date >= date_select.start_date
                and checkpoint_date <= date_select.end_date
        )
),

------ entry points ------
account_click as (
    select
        "timestamp" :: date event_date,
        "timestamp" event_timestamp,
        event,
        product,
        context_device_type,
        user_id
    from 
        ios.pfm_accounts_account_click
        cross join date_select
    where
        event_date >= date_select.start_date
        and event_date <= date_select.end_date
        and product in (
            'Credit Builder Plus',
            'Crypto',
            'Instacash',
            'Investment',
            'RoarMoney'
        )
    union all
    select
        "timestamp" :: date event_date,
        "timestamp" event_timestamp,
        event,
        product,
        context_device_type,
        user_id
    from 
        android.pfm_accounts_account_click
        cross join date_select
    where
        event_date >= date_select.start_date
        and event_date <= date_select.end_date
        and product in (
            'Credit Builder Plus',
            'Crypto',
            'Instacash',
            'Investment',
            'RoarMoney'
        )
),

loan_offer as (
    select
        "timestamp" :: date event_date,
        "timestamp" event_timestamp,
        event,
        partner,
        context_device_type,
        user_id
    from 
        ios.offer_status
        cross join date_select
    where
        event_date >= date_select.start_date
        and event_date <= date_select.end_date
        and interaction = 'engaged'
        and partner in (
            'Credit Builder Plus Loan',
            'Instacash℠ Advance'
        )
    union all
    select
        "timestamp" :: date event_date,
        "timestamp" event_timestamp,
        event,
        partner,
        context_device_type,
        user_id
    from 
        android.offer_status
        cross join date_select
    where
        event_date >= date_select.start_date
        and event_date <= date_select.end_date
        and interaction = 'engaged'
        and partner in (
            'Credit Builder Plus Loan',
            'Instacash℠ Advance'
        )
),

content_upsell as (
    select 
        "timestamp" :: date event_date,
        "timestamp" event_timestamp,
        event,
        coalesce(product, element_key) event_detail,
        -- element, element_key,
        context_device_type,
        user_id
    from 
        ios.content_status
        cross join date_select
    where
        event_date >= date_select.start_date
        and event_date <= date_select.end_date
        -- and element = 'Upsell-Card'
        and interaction = 'engaged'
    union all
    select 
        "timestamp" :: date event_date,
        "timestamp" event_timestamp,
        event,
        coalesce(product, element_key) event_detail,
        -- element, element_key,
        context_device_type,
        user_id
    from 
        android.content_status
        cross join date_select
    where
        event_date >= date_select.start_date
        and event_date <= date_select.end_date
        -- and element = 'Upsell-Card'
        and interaction = 'engaged'

),

screen_viewed as (
    select
        "timestamp" :: date event_date,
        "timestamp" event_timestamp,
        event,
        screen,
        context_device_type,
        user_id
    from 
        marketing.fct_screen_viewed 
        cross join date_select
    where
        "timestamp" :: date >= date_select.start_date
        and "timestamp" :: date <= date_select.end_date
        and screen in (
            'ShakeNudgeInvestment',
            'CreditMonitoringDashboard',
            'CryptoOnboardingLanding',
            'MembershipProfile'
        )
),

reward_banner as (
    select
        "timestamp" :: date event_date,
        "timestamp" event_timestamp,
        event,
        'reward_banner' as screen,
        context_device_type,
        user_id
    from 
        ios.rew_dda_cashback_banner_click
        cross join date_select
    where
        "timestamp" :: date >= date_select.start_date
        and "timestamp" :: date <= date_select.end_date
    union all
    select
        "timestamp" :: date event_date,
        "timestamp" event_timestamp,
        event,
        'reward_banner' as screen,
        context_device_type,
        user_id
    from 
        android.rew_dda_cashback_banner_click
        cross join date_select
    where
        "timestamp" :: date >= date_select.start_date
        and "timestamp" :: date <= date_select.end_date
),

referral_screen as (
    select
        "timestamp" :: date event_date,
        "timestamp" event_timestamp,
        event,
        'referral_screen' as screen,
        context_device_type,
        user_id
    from 
        ios.ref_main_view
        cross join date_select
    where
        "timestamp" :: date >= date_select.start_date
        and "timestamp" :: date <= date_select.end_date
    union all
    select
        "timestamp" :: date event_date,
        "timestamp" event_timestamp,
        event,
        'referral_screen' as screen,
        context_device_type,
        user_id
    from 
        android.ref_main_view
        cross join date_select
    where
        "timestamp" :: date >= date_select.start_date
        and "timestamp" :: date <= date_select.end_date
),

invest_widget as (
    select
        "timestamp" :: date event_date,
        "timestamp" event_timestamp,
        event,
        'invest_widget' as screen,
        context_device_type,
        user_id
    from 
        ios.wt_invest_tap_widget_engage
        cross join date_select
    where
        "timestamp" :: date >= date_select.start_date
        and "timestamp" :: date <= date_select.end_date
    union all
    select
        "timestamp" :: date event_date,
        "timestamp" event_timestamp,
        event,
        'invest_widget' as screen,
        context_device_type,
        user_id
    from 
        android.wt_invest_tap_widget_engage
        cross join date_select
    where
        "timestamp" :: date >= date_select.start_date
        and "timestamp" :: date <= date_select.end_date
),

entry_point as (
    select
        event_date,
        event_timestamp,
        event,
        product as event_detail,
        context_device_type as platform,
        user_id as user_email,

        ------ transformation ------
        case 
            ------ account_click ------
            when event_detail = 'Credit Builder Plus' then 'cbplus'
            when event_detail = 'Crypto' then 'crypto'
            when event_detail = 'Instacash' then 'instacash'
            when event_detail = 'Investment' then 'wealth'
            when event_detail = 'RoarMoney' then 'roarmoney'

            ------ loan_offer ------
            when event_detail = 'Instacash℠ Advance' then 'instacash'
            when event_detail = 'Credit Builder Plus Loan' then 'cbplus'

            ------ content_upsell ------
            when event_detail = 'Credit Builder Plus' then 'cbplus'
            when event_detail = 'Instacash' then 'instacash'
            when event_detail = 'Instacash advances' then 'instacash'
            when event_detail = 'Investment' then 'wealth'
            when event_detail = 'RoarMoney' then 'roarmoney'
            
            ------ screen_viewed ------
            when event_detail = 'ShakeNudgeInvestment' then 'wealth'
            when event_detail = 'CreditMonitoringDashboard' then 'credit tracking'
            when event_detail = 'CryptoOnboardingLanding' then 'crypto'
            when event_detail = 'MembershipProfile' then 'not defined'
            
            ------ individual events ------
            when event_detail = 'reward_banner' then 'roarmoney'
            when event_detail = 'referral_screen' then 'roarmoney'
            when event_detail = 'invest_widget' then 'wealth'

        end product_intent,

        case
            when event = 'screen_viewed' and event_detail = 'ShakeNudgeInvestment' then 'reward_shake_nudge'
            when event = 'screen_viewed' and event_detail = 'CreditMonitoringDashboard' then 'credit_monitoring'
            when event = 'screen_viewed' and event_detail = 'CryptoOnboardingLanding' then 'crypto_landing'
            when event = 'screen_viewed' and event_detail = 'MembershipProfile' then 'membership_profile'
            else event
        end entry_point
    from 
        (
            select * from account_click
            union all
            select * from loan_offer
            union all
            select * from content_upsell
            union all
            select * from screen_viewed
            union all
            select * from reward_banner
            union all
            select * from referral_screen
            union all
            select * from invest_widget
        )
),

------ user journey ------
user_core as (
    select
        gp_selection_focus_area.userid,
        gp_selection_focus_area.email,
        gp_selection_focus_area.lite_timestamp,
        gp_selection_focus_area.screen_timestamp,

        core_intro.name,
        core_intro.checkpoint_date,
        core_intro.checkpoint_timestamp,

        ------ transformation ------
        row_number() over(
            partition by gp_selection_focus_area.email
            order by
                core_intro.checkpoint_timestamp
        ) rn_core
    from 
        gp_selection_focus_area
        left join core_intro on gp_selection_focus_area.email = core_intro.user_id
),

user_journey as (
    select
        *,
        ------ transformation ------
                row_number() over(
                    partition by email
                    order by 
                        event_timestamp desc
                ) rn_engage
    from 
        (
            select
                user_core.userid,
                user_core.email,
                user_core.lite_timestamp,
                user_core.name,
                user_core.checkpoint_timestamp,

                entry_point.event_timestamp,
                entry_point.event,
                entry_point.event_detail,
                entry_point.product_intent,
                entry_point.entry_point

            from 
                user_core
                left join entry_point on (
                    user_core.email = entry_point.user_email
                    and user_core.checkpoint_date = entry_point.event_date
                    and user_core.checkpoint_timestamp >= entry_point.event_timestamp
                )
            where
                user_core.rn_core = 1
                and user_core.checkpoint_timestamp is not null 
        )
),

------ calculation ------

calculation as (
    select
        [checkpoint_timestamp:aggregation] as dt,
        entry_point,
        count(email) user_count
    from 
        user_journey
    where
        rn_engage = 1
    group by
        1, 2
),

total as (
    select
        dt,
        sum(user_count) total_user
    from
        calculation
    group by 1
),

final as (
    select
        calculation.dt,
        coalesce(entry_point, 'others') entry_point,
        user_count,
        (1.00 * user_count / total_user) user_pct
    from 
        calculation
        left join total on calculation.dt = total.dt
)

select * from final
order by 1