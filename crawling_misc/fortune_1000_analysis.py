
'''
lower(replace(company_name,' ','')) ilike '%walmart%' or lower(replace(company_name,' ','')) ilike '%exxonmobil%' or lower(replace(company_name,' ','')) ilike '%chevron%' or
lower(replace(company_name,' ','')) ilike '%berkshirehathaway%' or lower(replace(company_name,' ','')) ilike '%apple%' or
lower(replace(company_name,' ','')) ilike '%generalmotors%' or lower(replace(company_name,' ','')) ilike '%phillips66%' or
lower(replace(company_name,' ','')) ilike '%generalelectric%' or lower(replace(company_name,' ','')) ilike '%fordmotor%' or
lower(replace(company_name,' ','')) ilike '%cvshealth%' or lower(replace(company_name,' ','')) ilike '%mckesson%' or lower(replace(company_name,' ',''))
 ilike '%at&t%' or lower(replace(company_name,' ','')) ilike '%valeroenergy%' or lower(replace(company_name,' ','')) ilike
 '%unitedhealthgroup%' or lower(replace(company_name,' ','')) ilike '%verizon%' or lower(replace(company_name,' ','')) ilike
 '%amerisourcebergen%' or lower(replace(company_name,' ','')) ilike '%fanniemae%' or lower(replace(company_name,' ','')) ilike
 '%costco%' or lower(replace(company_name,' ','')) ilike '%hp%' or lower(replace(company_name,' ','')) ilike '%kroger%' or
 lower(replace(company_name,' ','')) ilike '%jpmorganchase%' or lower(replace(company_name,' ','')) ilike '%expressscriptsholding%'
 or lower(replace(company_name,' ','')) ilike '%bankofamericacorp.%' or lower(replace(company_name,' ','')) ilike '%ibm%' or
 lower(replace(company_name,' ','')) ilike '%marathonpetroleum%' or lower(replace(company_name,' ','')) ilike '%cardinalhealth%'
 or lower(replace(company_name,' ','')) ilike '%boeing%' or lower(replace(company_name,' ','')) ilike '%citigroup%' or lower(replace(company_name,' ',''))
 ilike '%amazon.com%' or lower(replace(company_name,' ','')) ilike '%wellsfargo%'


'%walmart%|%exxonmobil%|%chevron%|%berkshirehathaway%|%apple%|%generalmotors%|
%phillips66%|%generalelectric%|%fordmotor%|%cvshealth%|%mckesson%|%at&t%|
%valeroenergy%|%unitedhealthgroup%|%verizon%|%amerisourcebergen%|%fanniemae%|%costco%|hp|%kroger%'

select * from linkedin_company_base where
lower(replace(company_name,' ','')) similar to
'%walmart%|%exxonmobil%|%chevron%|%berkshirehathaway%|%apple%|%generalmotors%|
%phillips66%|%generalelectric%|%fordmotor%|%cvshealth%|%mckesson%|%at&t%|
%valeroenergy%|%unitedhealthgroup%|%verizon%|%amerisourcebergen%|%fanniemae%|%costco%|hp|%kroger%'
and company_size = '10,001+ employees'
 '''

import pandas as pd
import re
tmp = pd.read_excel('fortune1000_us_2015.xlsx')
names = list(tmp['name'])
names1 = ['%'+re.sub(' ','',i).lower()+'%' if len(re.sub(' ','',i))>3 else re.sub(' ','',i).lower() for i in names ]
names1 = [re.sub("'","''",i) for i in names1]

query_part = '|'.join(names1)

import psycopg2
con = psycopg2.connect(database='linkedin_data', user='postgres',password='$P$BptPVyArwpjzWXe1wz1cafxlpmVlGE',host='52.221.230.64')
query = "create table fortune_1000_companies as select * from linkedin_company_base where company_size = '10,001+ employees' and "\
        "lower(replace(company_name,' ','')) similar to '"+query_part+"' "
cursor = con.cursor()
cursor.execute(query)

'''
alter table fortune_1000_companies drop column timestamp
create table fortune_1000_companies_unique as select distinct * from fortune_1000_companies

create table fortune_1000_people as select b.* from
fortune_1000_companies_unique a join company_urls_mapper c on a.linkedin_url = c.alias_url
join people_company_mapper d on c.base_url=d.company_url join linkedin_people_base b on d.people_url = b.linkedin_url

create table fortune_1000_people_unique as
select distinct b.linkedin_url person_url,b.name, b.sub_text designation,location,a.linkedin_url as company_linkedin_url,
a.company_name, company_size, a.industry as company_industry,company_type,headquarters,website
 from
fortune_1000_companies_unique a join company_urls_mapper c on a.linkedin_url = c.alias_url
join people_company_mapper d on c.base_url=d.company_url join linkedin_people_base b on d.people_url = b.linkedin_url

select distinct person_url,name,company_name,designation,company_linkedin_url from fortune_1000_people_unique
where regexp_replace(lower(designation),' of | the | at | in |[^a-zA-Z]',' ','gi') similar to
'%chief learning%|%chief human resource%|%vp learning%|%vp training%|%vp talent management%|%director learning%|
%director training%|%director talent management%|%vice president learning%|%vice president training%|%vice president talent management%'
limit 100
'''