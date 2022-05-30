WITH core_or_plus as 
(
SELECT DISTINCT us.email 
FROM ml_public.subscription  s
INNER JOIN ml_public.usercollection us ON us.userid = s.userid
WHERE doc_type IN ('ML-Plus','ML-Free-Heavy')
  )



SELECT rr.createdon::date,
       rr.refereeemail,
       rr.referreremail
       
       
FROM ml_public.usercollection us 
INNER JOIN ml_public.referral  rr ON rr.referreremail= us.email
INNER JOIN ml_public.subscription s on s.userid = us.userid
where rr.referreremail IN (SELECT email FROM core_or_plus )
AND s.doc_type ='ML-Free-Heavy'
AND rr.createdon > '2019-01-20'
ORDER BY 1 DESC