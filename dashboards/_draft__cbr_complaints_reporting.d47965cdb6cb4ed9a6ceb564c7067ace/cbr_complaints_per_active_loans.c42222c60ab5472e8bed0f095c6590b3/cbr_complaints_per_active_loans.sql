with t1 as
(
SELECT 
  attributes__created_at, 
  id
from operations.kustomer_conversations
where complaints20_product_str = 'Credit Builder Plus'
and complaints20_product_cb_str = 'Credit Reporting'
), 

t2 as 
(
SELECT count(*) as loan_count
from ml_finance.fpall_ll
where leadtype like 'credit-builder-plus'
and loanstatus in ('New Loan','Returned Item')
)

select 
  [attributes__created_at:month],
  count(id) as cc,
  (select loan_count from t2) as lc,
  cc * 1.00 / lc as ratio 
from t1
where [attributes__created_at:month] > current_date - 180
group by 1
order by 1