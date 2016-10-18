-- tmp_ecommerce_not_dev_descr has few ecommerce companies

drop table if exists tmp_sample_ecom_rajesh;
create table tmp_sample_ecom_rajesh as
select distinct on (linkedin_url) b.* from tmp_ecommerce_not_dev_descr a join linkedin_company_base b using(linkedin_url);
create unique index on tmp_sample_ecom_rajesh (linkedin_url);

insert into tmp_sample_ecom_rajesh 
select * from linkedin_company_base where industry in ('Apparel & Fashion','Retail') 
on conflict do nothing;

-- add other details
drop table if exists tmp_sample_ecom_rajesh1;
create table tmp_sample_ecom_rajesh1 as 
SELECT distinct on (a.linkedin_url)
  a.linkedin_url, 
  a.company_name AS name, 
  a.company_size, 
  a.industry AS sector, 
  a.company_type, 
  a.headquarters, 
  a.description, 
  a.founded, 
  a.specialties, 
  a.website, 
  b.domain, 
  b.location, 
  b.region, 
  b.country, 
  c.technologies, 
  d.first_funding_at, 
  d.last_funding_at, 
  d.funding_rounds, 
  d.funding_total_usd AS funding_amount, 
  e.funded_at AS last_funded_at, 
  e.funding_round_type AS funding_level, 
  e.funding_round_code, 
  e.raised_amount_usd AS last_raised_amount_usd, 
  e.post_money_valuation_usd AS valuation
FROM 
  public.tmp_sample_ecom_rajesh a join 
  public.linkedin_company_domains b on  a.linkedin_url = b.linkedin_url  left join 
  public.builtwith_company_technologies c on b.domain = c.domain left join 
  public.cb_objects d on b.domain = d.domain left join 
  public.cb_funding_rounds e on d.id = e.object_id
where b.domain is not null and b.domain!='' and b.domain != 'NULL' and a.website != 'NULL'
order by a.linkedin_url,e.funded_at desc ;

--adding people
create table tmp_sample_ecom_rajesh1_people as 
select  distinct on (e.name,a.domain)
	e.name,trim(sub_text) as designation,
	 e.location as location_person,a.*,e.linkedin_url as people_linkedin_url
from 
tmp_sample_ecom_rajesh1 a join linkedin_company_domains b on a.linkedin_url=b.linkedin_url
join company_urls_mapper c on a.linkedin_url = c.alias_url join people_company_mapper d on c.base_url = d.company_url
join linkedin_people_base e on d.people_url = e.linkedin_url
where 
sub_text ~* '\yAVP.+Marketing\y|\yHead.+Web\y|\yHead.+Marketing\y|\yAVP.+Engineering\y|\yVP.+Product\y|\yHead.+Engineering\y|\yAVP.+Sales\y|\yCIO\y|\yChief Product Officer\y|\yPresident.+Marketing\y|\yVP.+Mobile\y|\yCTO\y|\ySVP.+Mobile\y|\yAVP.+Technology\y|\yVP.+Technology\y|\yCEO\y|\ySVP.+Product\y|\yPresident.+Product\y|\ySVP.+Marketing\y|\yDirector.+Technology\y|\yVP.+Sales\y|\yChief Executive Officer\y|\yPresident.+Inside Sales\y|\yAVP.+Inside Sales\y|\yPresident.+Sales\y|\yHead.+Mobile\y|\yDirector.+Engineering\y|\yDirector.+Sales\y|\y(Associate|Senior) Product Manager.+Web\y|\yChief Technology Officer\y|\yPresident.+Mobile\y|\yPresident.+Technology\y|\yCMO\y|\yChief Marketing Officer\y|\y(Head|AVP\y|VP\y|EVP\y|SVP\y|Director|President).+(Sales|Marketing|Digital Transformation|Sales Transformation|Field Management)\y|\yAVP.+Mobile\y|\yCofounder\y|\yVP.+Marketing\y|\yCo Founder\y|\yHead.+Sales\y|\yDirector.+Mobile\y|\yDirector.+Marketing\y|\ySVP.+Sales\y|\yFounder\y|\yDirector.+Inside Sales\y|\ySVP.+Engineering\y|\yHead.+Mobility\y|\yVP.+Engineering\y|\y(Sales|Marketing|Digital Transformation|Sales Transformation|Field Management).+(Head|AVP\y|VP\y|EVP\y|SVP\y|Director|President)\y|\y(Associate|Senior) Product Manager.+Mobile\y|\yPresident.+Engineering\y|\yHead.+Product\y|\yAVP.+Product\y|\yCo-founder\y|\yHead.+Technology\y|\yDirector.+Product\y|\ySVP.+Technology\y|\yCoFounder\y|\yChief Information Officer\y'
and sub_text ilike '%'||a.company_name||'%';



