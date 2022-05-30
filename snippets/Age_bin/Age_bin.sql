(CASE 
  WHEN age >= 0 and age < 20 THEN '0 - 20'
  WHEN age >= 20 and age < 25 THEN '20 - 24'
  WHEN age >= 25 and age < 30 THEN '25 - 29'
  WHEN age >= 30 and age < 35 THEN '30 - 34'
  WHEN age >= 35 and age < 40 THEN '35 - 39'
  WHEN age >= 40 and age < 45 THEN '40 - 44'
  WHEN age >= 45 and age < 50 THEN '45 - 49'
  WHEN age >= 50 and age < 55 THEN '50 - 54'
  WHEN age >= 55 THEN '55 and above'
END)