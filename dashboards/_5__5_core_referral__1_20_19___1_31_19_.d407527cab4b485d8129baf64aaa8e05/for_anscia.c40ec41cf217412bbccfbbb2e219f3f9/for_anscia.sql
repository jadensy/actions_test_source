WITH core_or_plus as 
(
SELECT DISTINCT us.email 
FROM ml_public.subscription  s
INNER JOIN ml_public.usercollection us ON us.userid = s.userid
WHERE doc_type IN ('ML-Plus','ML-Free-Heavy')
  )





SELECT referreremail as potential_award_winners, 
       createdon,
       Count 
FROM 
(

SELECT referreremail, 
       createdon,
       Count,
       row_number () Over (PARTITION by createdon order by Count DESC) as Rank
FROM 
(
SELECT COUNT(*) as Count, 
       referreremail,
       createdon
FROM 
(
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
  )
GROUP BY 2,3
  )
  )
where Rank = 1
order by 1 DESC