--ruby on rails 

create table rails_startups_people as
	select distinct b.company_name,b.domain,b.country_code,b.state_code,b.region,b.city,category_list,category_group_list,
	funding_rounds,funding_total_usd,founded_on,first_funding_on,last_funding_on,employee_count,b.email,b.phone,
	b.facebook_url,b.cb_url,b.twitter_url,first_name,last_name,primary_affiliation_title as designation,c.country_code country_code_person,c.state_code state_code_person,
	c.city city_person,c.twitter_url twitter_url_person,c.facebook_url facebook_url_person,c.cb_url cb_url_person
	from
	builtwith_company_technologies a join concierge_organizations_rails b on a.domain = b.domain
	left join crunchbase_data.people c on b.uuid = c.primary_organization_uuid
	where 'Ruby on Rails' = any(techs);

create table rails_startups_people_from_linkedin as
	select distinct b.company_name,b.domain,b.country_code,b.state_code,b.region,b.city,category_list,category_group_list,
	funding_rounds,funding_total_usd,founded_on,first_funding_on,last_funding_on,employee_count,b.email,b.phone,
	b.facebook_url,b.cb_url,b.twitter_url,name,trim(sub_text) as designation,location as location_person
	from
	builtwith_company_technologies a join concierge_organizations_rails b on a.domain = b.domain
	join linkedin_company_domains c on b.domain=c.domain join company_urls_mapper d on c.linkedin_url = d.alias_url
	join people_company_mapper e on d.base_url = e.company_url join linkedin_people_base f on e.people_url = f.linkedin_url
	where 'Ruby on Rails' = any(techs) and 
	sub_text ~* '\yAVP Engineering\y|\yAVP Marketing\y|\yAVP Mobile\y|\yAVP Product\y|\yAVP Technology\y|\yAssociate Product Manager\y|\yAssociate Product Manager Mobile\y|\yAssociate Product Manager Web\y|\yCEO\y|\yCIO\y|\yCMO\y|\yCTO\y|\yChief Executive Officer\y|\yChief Information Officer\y|\yChief Marketing Officer\y|\yChief Product Officer\y|\yChief Technology Officer\y|\yCo Founder\y|\yCo-founder\y|\yCoFounder\y|\yDirector Engineering\y|\yDirector Mobile\y|\yDirector Technology\y|\yFounder\y|\yHead of Mobility\y|\yHead of Product\y|\yManaging Director Engineering\y|\yManaging Director Mobile\y|\yManaging Director Technology\y|\yProduct Manager\y|\yProduct Manager Mobile\y|\yProduct Manager Web\y|\ySVP Engineering\y|\ySVP Marketing\y|\ySVP Mobile\y|\ySVP Product\y|\ySVP Technology\y|\ySenior Director Engineering\y|\ySenior Director Mobile\y|\ySenior Director Technology\y|\ySenior Product Manager\y|\ySenior Product Manager Mobile\y|\ySenior Product Manager Web\y|\yVP Engineering\y|\yVP Marketing\y|\yVP Mobile\y|\yVP Product\y|\yVP Technology\y|\yVP\y|\yAVP\y|\yEVP\y|\yHead\y|\yPresident\y|\yChief\y|\yGlobal\y'
;



--creating tables with matching conditions directly from base tables
drop table if exists concierge_linkedin_company_base_rails;
create table concierge_linkedin_company_base_rails_rails as
select * from concierge_linkedin_company_base_rails lk_c
where
 (lk_c.specialties ~* 'ruby on rails');


drop table if exists concierge_organizations_rails;
create table concierge_organizations_rails as
select * from
concierge_organizations_rails cb_c
where
cb_c.short_description ~* 'ruby on rails';

drop table if exists concierge_builtwith_companies_meta_data_rails;
create table concierge_builtwith_companies_meta_data_rails as 
  select bw_c.*,bw_c_t.techs from 
    concierge_builtwith_companies_meta_data_rails bw_c left join
  public.builtwith_company_technologies bw_c_t on bw_c."Domain" = bw_c_t.domain
  WHERE
'Ruby on Rails' = any(bw_c_t.techs);

--new queries

drop table if exists concierge_table_rubyonrails_1;
CREATE  TABLE concierge_table_rubyonrails_1 as 
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
  bw_c.techs technologies,
  cb_c.uuid
FROM 
  public.concierge_linkedin_company_base_rails lk_c join 
  public.linkedin_company_domains lk_cd_m on lk_c.linkedin_url = lk_cd_m.linkedin_url full outer join
  concierge_organizations_rails cb_c on lk_cd_m.domain = cb_c.domain full outer join
  concierge_builtwith_companies_meta_data_rails bw_c on cb_c.domain = bw_c."Domain" 
WHERE
  ((lk_cd_m.domain != '' and cb_c.domain != '' and bw_c."Domain" != '') or 
    (lk_cd_m.domain != '' and cb_c.domain != '') or 
    (cb_c.domain != '' and bw_c."Domain" != '') or
    (bw_c."Domain" != '' and lk_cd_m.domain != '')
  )
  ;

--insert from linkedin table###not right, we have to use company_domains table here
INSERT INTO concierge_table_rubyonrails_1 
(domain,company_name,employee_count,homepage_url,headquarters,industry,company_type,
  description,specialties,founded_on,linkedin_url)
SELECT DISTINCT ON (lower(company_name))
  replace(substring(website  from '.*://([^/]*)'),'www.','') as domain
  ,lower(company_name) as company_name,
  replace(company_size,' employees',''),
  website,headquarters,industry,company_type,description,specialties,founded,
  lk_c.linkedin_url as linkedin_url
FROM
  public.concierge_linkedin_company_base_rails lk_c
WHERE
   replace(substring(website  from '.*://([^/]*)'),'www.','')
   not in (SELECT DISTINCT domain FROM concierge_table_rubyonrails_1) and
  specialties ~* 'ruby on rails';

--insert from crunchbase table
INSERT INTO concierge_table_rubyonrails_1 
(domain,company_name,employee_count,primary_role,homepage_url,country,state,region,city,
  status,category_list,category_group_list,funding_rounds,funding_total_usd,description,
  founded_on,facebook_url,cb_url,twitter_url,email,phone,uuid)
SELECT DISTINCT ON (lower(company_name)) domain,lower(company_name) as company_name,
  employee_count,primary_role,homepage_url,
  country_code,state_code,region,city,status,category_list,category_group_list,funding_rounds,
  funding_total_usd,short_description,founded_on,facebook_url,cb_url,twitter_url,email,phone,uuid
FROM
  concierge_organizations_rails cb_c
WHERE
  company_name not in (select distinct company_name from concierge_table_colorado_1) and
  short_description ~* 'ruby on rails';

--insert from builtwith table
INSERT INTO concierge_table_rubyonrails_1
(domain,company_name,homepage_url,country,state,city,zipcode,vertical,facebook_url,twitter_url,alexa,
  linkedin_url,telephones_bw,emails_bw,people_bw,technologies)
SELECT DISTINCT ON (lower("Company")) "Domain",lower("Company") as company_name,
  "Location on Site","Country","State","City",
  "Zip","Vertical","Facebook","Twitter","Alexa","LinkedIn","Telephones","Emails","People",techs
FROM
  concierge_builtwith_companies_meta_data_rails bw_c 
WHERE
  "Domain" not in (select distinct domain from concierge_table_rubyonrails_1) and
  'Ruby on Rails' = any(bw_c.techs);

-- create table without duplicates
drop table if exists concierge_table_rubyonrails_2;
CREATE TABLE concierge_table_rubyonrails_2 as
SELECT DISTINCT ON (lower(company_name)) * FROM
concierge_table_rubyonrails_1;

--creating people
drop table if exists concierge_table_rubyonrails_3;
create table concierge_table_rubyonrails_3 as
select distinct * from (
(select a.*,b.first_name,'' as middle_name,b.last_name,
  b.primary_affiliation_title as designation,
   b.primary_affiliation_organization as company_person
 from
  concierge_table_rubyonrails_2 a join crunchbase_data.people b on
  a.uuid = b.primary_organization_uuid
WHERE
b.primary_affiliation_title  ~* '\yAVP Engineering\y|\yAVP Mobile\y|\yAVP Product\y|\yAVP Technology\y|\yProduct Manager\y|\yProduct Manager Mobile\y|\yAssociate Product Manager Web\y|\yCEO\y|\yCIO\y|\yCMO\y|\yCTO\y|\yChief Executive Officer\y|\yChief Information Officer\y|\yChief Product Officer\y|\yChief Technology Officer\y|\yCo Founder\y|\yCo-founder\y|\yCoFounder\y|\yDirector Engineering\y|\yDirector Mobile\y|\yDirector Technology\y|\yFounder\y|\yHead.+Mobility\y|\yHead.+Mobile\y|\yHead.+Engineering\y|\yHead.+Product\y|\yDirector.+Engineering\y|\yDirector.+Mobile\y|\yDirector.+Technology\y|\yProduct Manager.+Mobile\y|\yProduct Manager Web\y|\ySVP.+Engineering\y|\ySVP.+Mobile\y|\ySVP.+Product\y|\ySVP.+Technology\y|\yDirector.+Engineering\y|\yDirector.+Mobile\y|\yDirector.+Technology\y|\ySenior Product Manager\y|\ySenior Product Manager.+Mobile\y|\ySenior Product Manager Web\y|\yVP.+Engineering\y|\yVP.+Mobile\y|\yVP.+Product\y|\yVP.+Technology\y'
  )
UNION
 (select a.*,(name_cleaner(name))[2] as first_name,
 (name_cleaner(name))[3] as middle_name,
 (name_cleaner(name))[4] as last_name,
 trim(sub_text) as designation,b.company_name as  company_person
 from
 concierge_table_rubyonrails_2 a join people_company_mapper c on a.linkedin_url = c.company_url
 join linkedin_people_base b on c.people_url = b.linkedin_url
 WHERE
 sub_text ~* '\yAVP Engineering\y|\yAVP Mobile\y|\yAVP Product\y|\yAVP Technology\y|\yProduct Manager\y|\yProduct Manager Mobile\y|\yAssociate Product Manager Web\y|\yCEO\y|\yCIO\y|\yCMO\y|\yCTO\y|\yChief Executive Officer\y|\yChief Information Officer\y|\yChief Product Officer\y|\yChief Technology Officer\y|\yCo Founder\y|\yCo-founder\y|\yCoFounder\y|\yDirector Engineering\y|\yDirector Mobile\y|\yDirector Technology\y|\yFounder\y|\yHead.+Mobility\y|\yHead.+Mobile\y|\yHead.+Engineering\y|\yHead.+Product\y|\yDirector.+Engineering\y|\yDirector.+Mobile\y|\yDirector.+Technology\y|\yProduct Manager.+Mobile\y|\yProduct Manager Web\y|\ySVP.+Engineering\y|\ySVP.+Mobile\y|\ySVP.+Product\y|\ySVP.+Technology\y|\yDirector.+Engineering\y|\yDirector.+Mobile\y|\yDirector.+Technology\y|\ySenior Product Manager\y|\ySenior Product Manager.+Mobile\y|\ySenior Product Manager Web\y|\yVP.+Engineering\y|\yVP.+Mobile\y|\yVP.+Product\y|\yVP.+Technology\y'
 )
)x;
