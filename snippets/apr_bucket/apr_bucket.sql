case
  when floor(apr * 100) < 9 then '000) 0 > apr < 9'
  when floor(apr * 100) between 9 and 10 then '001) 9 >= apr <= 10'
  when floor(apr * 100) between 11 and 50 then '002) 11 >= apr <= 50'
  when floor(apr * 100) between 51 and 100 then '003) 51 >= apr <= 100'
  when floor(apr * 100) between 101 and 200 then '004) 101 >= apr <= 200'
  when floor(apr * 100) between 201 and 300 then '005) 201 >= apr <= 300'
  when floor(apr * 100) between 301 and 400 then '006) 301 >= apr <= 400'
  when floor(apr * 100) between 401 and 500 then '007) 401 >= apr <= 500'
  when floor(apr * 100) between 501 and 600 then '008) 501 >= apr <= 600'
  when floor(apr * 100) between 601 and 700 then '009) 601 >= apr <= 700'
  else '010) apr > 700'
end