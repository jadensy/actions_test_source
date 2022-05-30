SELECT
id,
created_at as "Created Date",
complaints_2_0_full_name as "Requester Name", 
complaints_2_0_product_overtime as "Product, BNPL",
complaints_2_0_complaint_description as "Complaints 2.0 - Complaint Description", 
--complaints_2_0_desired_resolution as "Complaints 2.0 - Desired Resolution",
--complaints_2_0_resolution_summary as "Resolution Summary",
complaints_2_0_mldk_ticket

from zendesk.tickets
where complaints_2_0_complaint_type in ('regulatory','non_regulatory')
--and created_at >= current_date - 60
and complaints_2_0_product = 'overtime__buy_now_pay_later_'
and complaints_2_0_product_overtime is not null
order by 2 desc