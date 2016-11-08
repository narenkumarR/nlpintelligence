--creating table from linkedin_company_base
drop table if exists tmp_madstreetden_nov07;
create table tmp_madstreetden_nov07 as 
select distinct on (a.linkedin_url)  a.*,category_cleaned,unique_visitors,global_rank from linkedin_company_base a 
 join linkedin_company_domains b on a.linkedin_url=b.linkedin_url
 join ecommerce_companies_alexa_data_unique c on lower(b.domain)=lower(c.site)
where 
b.country in ('UNITED STATES','BRAZIL') and (category_cleaned in ('Beauty_Products','Clothing','Consumer_Electronics','Entertainment','General_Merchandise','Home_and_Garden','Office_Products','Recreation','Sporting_Goods',
'Toys_and_Games','Weddings') or industry in ('Apparel & Fashion','Cosmetics','Design','Retail','Textiles','Wholesale'))
and (unique_visitors between 1000000 and 12000000 or global_rank < 30000)
;

--creating people table
drop table if exists tmp_madstreetden_nov07_people;
create table tmp_madstreetden_nov07_people as 
select distinct on (name,domain) * from (
(select  distinct on (e.name,b.domain)
	e.name,trim(sub_text) as designation,b.domain,a.company_name,a.website as company_website,
	b.headquarters,b.country,b.region,b.location location_company,founded,company_size,a.industry,trim(a.specialties) as specialties,
	trim(a.description) description, e.location as location_person,a.linkedin_url as company_linkedin_url,e.linkedin_url as people_linkedin_url,
	category_cleaned,unique_visitors,global_rank
from 
tmp_madstreetden_nov07 a join linkedin_company_domains b on a.linkedin_url=b.linkedin_url
join company_urls_mapper c on a.linkedin_url = c.alias_url join people_company_mapper d on c.base_url = d.company_url
join linkedin_people_base e on d.people_url = e.linkedin_url
where 
sub_text ~* '\yProduct manager\y|\yManager.+Search Recommendation\y|\ySearch Recommendation.+manager\y|\y(Head|AVP\y|VP\y|EVP\y|SVP\y|Director|President).+(Product|Category)\y|\y(Product|Category).+(Head|\yAVP|\yVP|\yEVP|\ySVP|Director|President)\y|\yC[EMOTI]O\y|\yChief (Executive|Information|Marketing|Product|Technology|Operation).+Officer\y|\y(Sr(.)?\y|senior) product manager\y|\y(Sr(.)?\y|senior) Marketing manager\y|\yvisual merchandiser\y'
and sub_text ilike '%'||a.company_name||'%')
union
--using experience , get roles and companies and get matching people
(select distinct on (x.name,x.domain)
x.name,x.designation,x.domain,x.company_name,x.company_website,x.headquarters,x.country,x.region,x.location_company,
x.founded,x.company_size,x.industry,x.specialties,
x.description,x.location_person,x.company_linkedin_url,x.people_linkedin_url,
category_cleaned,unique_visitors,global_rank
from 
(select  
	e.name,b.domain,a.company_name,a.website as company_website,
	b.headquarters,b.country,b.region,b.location location_company,founded,company_size,a.industry,trim(a.specialties) as specialties,
	trim(a.description) description, e.location as location_person,e.linkedin_url as people_linkedin_url,
	trim(unnest(clean_linkedin_url_array(extract_related_info(experience_array,1)))) company_linkedin_url,
	trim(unnest(clean_linkedin_url_array(extract_related_info(experience_array,2)))) designation,
	trim(unnest(clean_linkedin_url_array(extract_related_info(experience_array,4)))) work_time,
	a.linkedin_url as company_linkedin_url_orig
	from
	tmp_madstreetden_nov07 a join linkedin_company_domains b on a.linkedin_url=b.linkedin_url
	join company_urls_mapper c on a.linkedin_url = c.alias_url join people_company_mapper d on c.base_url = d.company_url
	join linkedin_people_base e on d.people_url = e.linkedin_url 
	where 
	experience_array[1] like '%{}%'  
)x
join
tmp_madstreetden_nov07 y on x.company_linkedin_url = y.linkedin_url
where
designation ~* '\yProduct manager\y|\yManager.+Search Recommendation\y|\ySearch Recommendation.+manager\y|\y(Head|AVP\y|VP\y|EVP\y|SVP\y|Director|President).+(Product|Category)\y|\y(Product|Category).+(Head|\yAVP|\yVP|\yEVP|\ySVP|Director|President)\y|\yC[EMOTI]O\y|\yChief (Executive|Information|Marketing|Product|Technology|Operation).+Officer\y|\y(Sr(.)?\y|senior) product manager\y|\y(Sr(.)?\y|senior) Marketing manager\y|\yvisual merchandiser\y'
and work_time like '%Present%' and company_linkedin_url = company_linkedin_url_orig)
union
--people with single url in company linkedin url take directly
(select  distinct on (e.name,b.domain)
	e.name,trim(sub_text) as designation,b.domain,a.company_name,a.website as company_website,
	b.headquarters,b.country,b.region,b.location location_company,founded,company_size,a.industry,trim(a.specialties) as specialties,
	trim(a.description) description, e.location as location_person,a.linkedin_url as company_linkedin_url,e.linkedin_url as people_linkedin_url,
	category_cleaned,unique_visitors,global_rank
from 
tmp_madstreetden_nov07 a join linkedin_company_domains b on a.linkedin_url=b.linkedin_url
join company_urls_mapper c on a.linkedin_url = c.alias_url join people_company_mapper d on c.base_url = d.company_url
join linkedin_people_base e on d.people_url = e.linkedin_url
where 
sub_text ~* '\yProduct manager\y|\yManager.+Search Recommendation\y|\ySearch Recommendation.+manager\y|\y(Head|AVP\y|VP\y|EVP\y|SVP\y|Director|President).+(Product|Category)\y|\y(Product|Category).+(Head|\yAVP|\yVP|\yEVP|\ySVP|Director|President)\y|\yC[EMOTI]O\y|\yChief (Executive|Information|Marketing|Product|Technology|Operation).+Officer\y|\y(Sr(.)?\y|senior) product manager\y|\y(Sr(.)?\y|senior) Marketing manager\y|\yvisual merchandiser\y'
and array_length(company_linkedin_url_array,1) = 1 and (company_linkedin_url_array[1]=c.base_url or company_linkedin_url_array[1]=c.alias_url)
)
)x;

