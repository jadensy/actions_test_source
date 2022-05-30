with cte_a as (
select name as institution_name, 
case when providerinstid_yodlee_id <> '' then 'yodlee' else '' end as "provider_name",
case when providerinstid_yodlee_id <> '' then providerinstid_yodlee_id else '' end as "provider_id"
from ml_public.institution_id_ref

union 

select name as institution_name, 
case when providerinstid_quovo_id <> '' then 'quovo' else '' end as "provider_name",
case when providerinstid_quovo_id <> '' then providerinstid_quovo_id else '' end as "provider_id"
from ml_public.institution_id_ref

union

select name as institution_name, 
case when providerinstid_plaid_id <> '' then 'plaid' else '' end as "provider_name",
case when providerinstid_plaid_id <> '' then providerinstid_plaid_id else '' end as "provider_id"
from ml_public.institution_id_ref

union

select name as institution_name, 
case when providerinstid_plaid_id_v2 <> '' then 'plaidlink' else '' end as "provider_name",
case when providerinstid_plaid_id_v2 <> '' then providerinstid_plaid_id_v2 else '' end as "provider_id"
from ml_public.institution_id_ref
  
union
  
SELECT name AS institution_name
, case when provider = 'plaid' then 'plaidlink' else provider end as "provider_name"
, inst_id::VARCHAR AS "provider_id"
FROM ml_public.mysql_institution_provider
)

SELECT DISTINCT *
FROM cte_a 
WHERE provider_name <> ''