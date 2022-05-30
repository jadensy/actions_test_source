/*
MAIN: LITE 2.0 JOURNEY ANALYSIS
*/

WITH date_select AS (
    SELECT
        '2022-01-25' :: date start_date,
        ([daterange_end] - INTERVAL '3 day') :: date end_date
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
        end entry_point,

        case
            when event = 'screen_viewed' and event_detail = 'ShakeNudgeInvestment' then 'rewards'
            when event = 'screen_viewed' and event_detail = 'CreditMonitoringDashboard' then 'credit_monitoring'
            when event = 'screen_viewed' and event_detail = 'CryptoOnboardingLanding' then 'crypto'
            when event = 'screen_viewed' and event_detail = 'MembershipProfile' then 'profile'
            when event = 'content_status' then 'today'
            when event = 'offer_status' then 'loan'
            when event = 'pfm_accounts_account_click' then 'accounts'
            when event = 'ref_main_view' then 'referral'
            when event = 'rew_dda_cashback_banner_click' then 'rewards'
            when event = 'wt_invest_tap_widget_engage' then 'today'
            else 'others'
        end app_location
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
                entry_point.entry_point,
                entry_point.app_location

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


------ cohort ffa from backend membership ------
ffa_membership as (
    SELECT
        userid,
        email,
        first_date,
        first_timestamp,
        product,
        product_detail,
        source,
        source_breakdown,
        last_update_timestamp,
        rn
    FROM
        marketing.stg_user__membership__merged_with_aff
        CROSS JOIN date_select
    WHERE
        product not in ('lite', 'new core')
        and rn = 1
        and first_date >= date_select.start_date
),

user_journey_ffa as (
    select
        user_journey.userid,
        user_journey.email,
        user_journey.lite_timestamp,
        user_journey.name,
        user_journey.checkpoint_timestamp,
        user_journey.event_timestamp,
        user_journey.event,
        user_journey.event_detail,
        user_journey.product_intent,
        user_journey.entry_point,
        user_journey.app_location,

        ffa_membership.product as ffa_product,
        ffa_membership.first_timestamp as ffa_timestamp

    from 
        user_journey
        left join ffa_membership on user_journey.userid = ffa_membership.userid
    where
        rn_engage = 1
),

------ calculation ------

calculation as (
    select
        coalesce(entry_point, 'others') entry_point,
        count(email) core_intro,
        count(ffa_timestamp) ffa
    from 
        user_journey_ffa
    group by 
        1
),

total as (
    select
        sum(core_intro) total_entry,
        sum(ffa) total_ffa
    from 
        calculation
),

final as (
    select
        calculation.entry_point,
        calculation.core_intro,
        calculation.ffa,
        (1.00 * core_intro / total_entry) core_intro_pct,
        (1.00 * ffa / core_intro) intro_to_ffa
  
    from 
        calculation
        cross join total
)

select * from final
order by 2 desc