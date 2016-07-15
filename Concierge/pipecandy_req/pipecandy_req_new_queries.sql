
--creating tables with matching conditions directly from base tables
drop table if exists concierge_linkedin_company_base_pipecandyreq;
create table concierge_linkedin_company_base_pipecandyreq as
select * from linkedin_company_base lk_c
where
 lk_c.description ~* 'saas' or lk_c.specialties ~* 'saas';

drop table if exists concierge_organizations_pipecandyreq;
create table concierge_organizations_pipecandyreq as
select * from
crunchbase_data.organizations cb_c
where
cb_c.short_description ~* 'saas' or cb_c.category_list ~* 'saas';

--merging results
drop table if exists concierge_table_pipecandyreq_1;
CREATE  TABLE concierge_table_pipecandyreq_1 as
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
  public.concierge_linkedin_company_base_pipecandyreq lk_c join
  public.linkedin_company_domains lk_cd_m on lk_c.linkedin_url = lk_cd_m.linkedin_url full outer join
  concierge_organizations_pipecandyreq cb_c on lk_cd_m.domain = cb_c.domain left join
  public.builtwith_companies_meta_data bw_c on cb_c.domain = bw_c."Domain" left join
  public.builtwith_company_technologies bw_c_t on bw_c."Domain" = bw_c_t.domain
WHERE
  ((lk_cd_m.domain != '' and cb_c.domain != '' and bw_c."Domain" != '') or
    (lk_cd_m.domain != '' and cb_c.domain != '') or
    (cb_c.domain != '' and bw_c."Domain" != '') or
    (bw_c."Domain" != '' and lk_cd_m.domain != '')
  ) and
  ((cb_c.short_description ~* 'saas|b2b' or cb_c.category_list ~* 'saas|b2b') or
	( lk_c.description ~* 'saas|b2b' or lk_c.specialties ~* 'saas|b2b'))
  ;

--insert from linkedin table
INSERT INTO concierge_table_pipecandyreq_1
(domain,company_name,employee_count,homepage_url,headquarters,industry,company_type,
  description,specialties,founded_on,linkedin_url)
SELECT DISTINCT ON (lower(company_name))
  replace(substring(website  from '.*://([^/]*)'),'www.','') as domain
  ,lower(company_name) as company_name,
  replace(company_size,' employees',''),
  website,headquarters,industry,company_type,description,specialties,founded,
  lk_c.linkedin_url as linkedin_url
FROM
  public.concierge_linkedin_company_base_pipecandyreq lk_c
WHERE
   replace(substring(website  from '.*://([^/]*)'),'www.','')
   not in (SELECT DISTINCT domain FROM concierge_table_pipecandyreq_1) and
  (lk_c.description ~* 'saas|b2b' or lk_c.specialties ~* 'saas|b2b');

--insert from crunchbase table
INSERT INTO concierge_table_pipecandyreq_1
(domain,company_name,employee_count,primary_role,homepage_url,country,state,region,city,
  status,category_list,category_group_list,funding_rounds,funding_total_usd,description,
  founded_on,facebook_url,cb_url,twitter_url,email,phone,uuid)
SELECT DISTINCT ON (lower(company_name)) domain,lower(company_name) as company_name,
  employee_count,primary_role,homepage_url,
  country_code,state_code,region,city,status,category_list,category_group_list,funding_rounds,
  funding_total_usd,short_description,founded_on,facebook_url,cb_url,twitter_url,email,phone,uuid
FROM
  concierge_organizations_pipecandyreq cb_c
WHERE
  company_name not in (select distinct company_name from concierge_table_pipecandyreq_1) and
 (cb_c.short_description ~* 'saas|b2b' or cb_c.category_list ~* 'saas|b2b') ;

-- create table without duplicates
drop table if exists concierge_table_pipecandyreq_2;
CREATE TABLE concierge_table_pipecandyreq_2 as
SELECT DISTINCT ON (company_name) * FROM
concierge_table_pipecandyreq_1;

--creating people
drop table if exists concierge_table_pipecandyreq_3;
create table concierge_table_pipecandyreq_3 as
select distinct * from (
(select a.*,b.first_name,'' as middle_name,b.last_name,
  b.primary_affiliation_title as designation,
   b.primary_affiliation_organization as company_person
 from
  concierge_table_pipecandyreq_2 a join crunchbase_data.people b on
  a.uuid = b.primary_organization_uuid
  where
  b.primary_affiliation_title ~* '\yAVP.+Marketing\y|\yAVP.+Sales\y|\yCEO\y|\yCOO\y|\yCIO\y|\yCMO\y|\yChief Executive Officer\y|\yChief Information Officer\y|\yChief Marketing Officer\y|\yChief Sales Officer\y|\yChief Business Development\y|\yCo Founder\y|\yCo-founder\y|\yCoFounder\y|\yFounder\y|\ySVP.+Marketing\y|\ySVP.+Sales\y|\yVP.+Marketing\y|\yVP.+Sales\y|\yPresident.+Marketing\y|\yPresident.+Sales\y'
  )
UNION
 (select a.*,(name_cleaner(name))[2] as first_name,
 (name_cleaner(name))[3] as middle_name,
 (name_cleaner(name))[4] as last_name,
 trim(sub_text) as designation,b.company_name as  company_person
 from
 concierge_table_pipecandyreq_2 a join people_company_mapper c on a.linkedin_url = c.company_url
 join linkedin_people_base b on c.people_url = b.linkedin_url
 WHERE
 sub_text ~* '\yAVP.+Marketing\y|\yAVP.+Sales\y|\yCEO\y|\yCOO\y|\yCIO\y|\yCMO\y|\yChief Executive Officer\y|\yChief Information Officer\y|\yChief Marketing Officer\y|\yChief Sales Officer\y|\yChief Business Development\y|\yCo Founder\y|\yCo-founder\y|\yCoFounder\y|\yFounder\y|\ySVP.+Marketing\y|\ySVP.+Sales\y|\yVP.+Marketing\y|\yVP.+Sales\y|\yPresident.+Marketing\y|\yPresident.+Sales\y'
 )
)x;
