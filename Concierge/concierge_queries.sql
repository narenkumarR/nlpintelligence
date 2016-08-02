
drop table if exists concierge_table_key_1;
CREATE  TABLE concierge_table_key_1 as 
SELECT DISTINCT ON (cb_c.domain)
  cb_c.domain, 
  lower(cb_c.company_name) as company_name,
  case when cb_c.employee_count != '' then cb_c.employee_count else replace(lk_c.company_size,' employees','') end as employee_count, 
  cb_c.primary_role, 
  cb_c.homepage_url, 
  case when cb_c.country_code != '' then cb_c.country_code else bw_c."Country" end as country, 
  case when cb_c.state_code != '' then cb_c.state_code else bw_c."State" end as state, 
  cb_c.region, 
  case when cb_c.city != '' then cb_c.city else bw_c."City" end as city, 
  bw_c."Zip" as zipcode,
  lk_c.headquarters, 
  cb_c.status, 
  lk_c.industry, 
  cb_c.category_list,
  cb_c.category_group_list,
  bw_c."Vertical" as vertical,
  lk_c.company_type, 
  cb_c.funding_rounds,
  cb_c.funding_total_usd,
  lk_c.description::text || '. '|| cb_c.short_description::text as description, 
  lk_c.specialties, 
  case when cb_c.founded_on != '' then cb_c.founded_on else lk_c.founded end as founded_on, 
  case when cb_c.facebook_url != '' then cb_c.facebook_url else bw_c."Facebook" end as facebook_url, 
  cb_c.cb_url, 
  case when cb_c.twitter_url != '' then cb_c.twitter_url else bw_c."Twitter" end as twitter_url,
  bw_c."Alexa" as alexa, 
  lk_c.linkedin_url, 
  cb_c.email, 
  cb_c.phone, 
  bw_c."Telephones" telephones_bw, 
  bw_c."Emails" emails_bw, 
  bw_c."People" people_bw,  
  bw_c_t.techs technologies,
  cb_c.uuid
FROM 
  public.linkedin_company_base lk_c join 
  public.linkedin_company_domains lk_cd_m on lk_c.linkedin_url = lk_cd_m.linkedin_url join
  crunchbase_data.organizations cb_c on lk_cd_m.domain = cb_c.domain join
  public.builtwith_companies_meta_data bw_c on cb_c.domain = bw_c."Domain" left join
  public.builtwith_company_technologies bw_c_t on bw_c."Domain" = bw_c_t.domain
WHERE
  lk_cd_m.domain != '' and
  cb_c.domain != '' and 
  bw_c."Domain" != '' and
  conditions

--insert from linkedin table
INSERT INTO concierge_table_key_1 
(domain,company_name,employee_count,homepage_url,headquarters,industry,company_type,
  description,specialties,founded_on,linkedin_url)
SELECT DISTINCT ON (lower(company_name))
  replace(substring(website  from '.*://([^/]*)'),'www.','') as domain
  ,lower(company_name) as company_name,
  replace(company_size,' employees',''),
  website,headquarters,industry,company_type,description,specialties,founded,
  lk_c.linkedin_url as linkedin_url
FROM
  public.linkedin_company_base lk_c
WHERE
   replace(substring(website  from '.*://([^/]*)'),'www.','')
   not in (SELECT DISTINCT domain FROM concierge_table_key_1) and
  conditions;

--insert from crunchbase table
INSERT INTO concierge_table_key_1 
(domain,company_name,employee_count,primary_role,homepage_url,country,state,region,city,
  status,category_list,category_group_list,funding_rounds,funding_total_usd,description,
  founded_on,facebook_url,cb_url,twitter_url,email,phone,uuid)
SELECT DISTINCT ON (lower(company_name)) domain,lower(company_name) as company_name,
  employee_count,primary_role,homepage_url,
  country_code,state_code,region,city,status,category_list,category_group_list,funding_rounds,
  funding_total_usd,short_description,founded_on,facebook_url,cb_url,twitter_url,email,phone,uuid
FROM
  crunchbase_data.organizations cb_c
WHERE
  company_name not in (select distinct company_name from concierge_table_key_1) and
  conditions;

--insert from builtwith table
INSERT INTO concierge_table_key_1
(domain,company_name,homepage_url,country,state,city,zipcode,vertical,facebook_url,twitter_url,alexa,
  linkedin_url,telephones_bw,emails_bw,people_bw,technologies)
SELECT DISTINCT ON (lower("Company")) "Domain",lower("Company") as company_name,
  "Location on Site","Country","State","City",
  "Zip","Vertical","Facebook","Twitter","Alexa","LinkedIn","Telephones","Emails","People",techs
FROM
  builtwith_companies_meta_data bw_c left join
  builtwith_company_technologies bw_c_t on bw_c."Domain" = bw_c_t.domain
WHERE
  "Company" not in (select distinct company_name from concierge_table_key_1) and
  conditions;

-- create table without duplicates
drop table if exists concierge_table_key_2;
CREATE TABLE concierge_table_key_2 as
SELECT DISTINCT ON (company_name) * FROM
concierge_table_key_1;

--creating people
create table concierge_table_key_3 as
select distinct * from (
(select a.*,b.first_name,'' as middle_name,b.last_name,
  b.primary_affiliation_title as designation,
   b.primary_affiliation_organization as company_person
 from
  concierge_table_key_2 a join crunchbase_data.people b on
  a.uuid = b.primary_organization_uuid)
UNION
 (select a.*,(name_cleaner(name))[2] as first_name,
 (name_cleaner(name))[3] as middle_name,
 (name_cleaner(name))[4] as last_name,
 sub_text as designation,b.company_name as  company_person
 from
 concierge_table_key_2 a join people_company_mapper c on a.linkedin_url = c.company_url
 join linkedin_people_base b on c.people_url = b.linkedin_url
 )
)x;
