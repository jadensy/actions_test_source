select 
	fct.partner_service_name,
	(case when assignment_group = '-1' then 'control (default Contentful)'
        when assignment_group = '0' then 'control-control (AIDE inject)'
        when assignment_group = '1' then 'treatment group' end) as assignment_group_name,
    count(distinct fct.userid) as unique_conversions,
    count(fct.userid) as total_conversions,
	round(sum(coalesce(l30d_avg_revenue_per_conversion,0))::float,2) as total_norm_rev
from marketplace_static.exp_ooo_model_v0_20220107 as exp_user 
join marketplace.fct_conversion_revenue as fct
  	on fct.userid  = exp_user.user_id and fct.clicked_at >= exp_user.injection_time
left join marketplace_static.partner_avg_revenue_per_conversion as partner_rev
	on fct.partner_service_name = partner_rev.partner_service_name and fct.converted_at::date = partner_rev.conversion_date
where medium = 'marketplace'
and clicked_at >= '2022-01-07' and clicked_at <= '2022-01-26'
group by 1, 2
order by total_norm_rev desc