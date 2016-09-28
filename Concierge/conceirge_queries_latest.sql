--change table names and requirements accordingly
--creating table from linkedin_company_base
drop table if exists tmp_aus_travel;
create table tmp_aus_travel as 
select distinct on (b.linkedin_url)  b.* from linkedin_company_domains a join linkedin_company_base b using(linkedin_url)
where a.country = 'AUSTRALIA' and industry = 'Leisure, Travel & Tourism';

--creating people table
create table tmp_aus_travel_people as 
select distinct on (name,domain) * from (
(select  distinct on (e.name,b.domain)
	e.name,trim(sub_text) as designation,b.domain,a.company_name,a.website as company_website,
	b.headquarters,b.country,b.region,b.location location_company,founded,company_size,a.industry,trim(a.specialties) as specialties,
	trim(a.description) description, e.location as location_person,a.linkedin_url as company_linkedin_url,e.linkedin_url as people_linkedin_url
from 
tmp_aus_travel a join linkedin_company_domains b on a.linkedin_url=b.linkedin_url
join company_urls_mapper c on a.linkedin_url = c.alias_url join people_company_mapper d on c.base_url = d.company_url
join linkedin_people_base e on d.people_url = e.linkedin_url
where 
sub_text ~* '\yC[EIT]O\y|\yChief (Executive|Information|Product(|s\y)|Technology).+Officer\y|\yCo(|-| )Founder\y|\yFounder\y|\y(Head|AVP\y|VP\y|EVP\y|SVP\y|Director|President).+(\yIT|Information Technology|Mobility|Product(|s\y))\y|\y(\yIT|Information Technology|Mobility|Product(|s\y)).+(Head|AVP\y|VP\y|EVP\y|SVP\y|Director|President)\y'
and sub_text ilike '%'||a.company_name||'%')
union
--using experience , get roles and companies and get matching people
(select distinct on (x.name,x.domain)
x.name,x.designation,x.domain,x.company_name,x.company_website,x.headquarters,x.country,x.region,x.location_company,
x.founded,x.company_size,x.industry,x.specialties,
x.description,x.location_person,x.company_linkedin_url,x.people_linkedin_url
from 
(select  
	e.name,b.domain,a.company_name,a.website as company_website,
	b.headquarters,b.country,b.region,b.location location_company,founded,company_size,a.industry,trim(a.specialties) as specialties,
	trim(a.description) description, e.location as location_person,e.linkedin_url as people_linkedin_url,
	trim(unnest(clean_linkedin_url_array(extract_related_info(experience_array,1)))) company_linkedin_url,
	trim(unnest(clean_linkedin_url_array(extract_related_info(experience_array,2)))) designation,
	trim(unnest(clean_linkedin_url_array(extract_related_info(experience_array,4)))) work_time
	from
	tmp_aus_travel a join linkedin_company_domains b on a.linkedin_url=b.linkedin_url
	join company_urls_mapper c on a.linkedin_url = c.alias_url join people_company_mapper d on c.base_url = d.company_url
	join linkedin_people_base e on d.people_url = e.linkedin_url 
	where 
	experience_array[1] like '%{}%'  
)x
join
tmp_aus_travel y on x.company_linkedin_url = y.linkedin_url
where
designation ~* '\yC[EIT]O\y|\yChief (Executive|Information|Product(|s\y)|Technology).+Officer\y|\yCo(|-| )Founder\y|\yFounder\y|\y(Head|AVP\y|VP\y|EVP\y|SVP\y|Director|President).+(\yIT|Information Technology|Mobility|Product(|s\y))\y|\y(\yIT|Information Technology|Mobility|Product(|s\y)).+(Head|AVP\y|VP\y|EVP\y|SVP\y|Director|President)\y'
and work_time like '%Present%' )
union
--people with single url in company linkedin url take directly
(select  distinct on (e.name,b.domain)
	e.name,trim(sub_text) as designation,b.domain,a.company_name,a.website as company_website,
	b.headquarters,b.country,b.region,b.location location_company,founded,company_size,a.industry,trim(a.specialties) as specialties,
	trim(a.description) description, e.location as location_person,a.linkedin_url as company_linkedin_url,e.linkedin_url as people_linkedin_url
from 
tmp_aus_travel a join linkedin_company_domains b on a.linkedin_url=b.linkedin_url
join company_urls_mapper c on a.linkedin_url = c.alias_url join people_company_mapper d on c.base_url = d.company_url
join linkedin_people_base e on d.people_url = e.linkedin_url
where 
sub_text ~* '\yC[EIT]O\y|\yChief (Executive|Information|Product(|s\y)|Technology).+Officer\y|\yCo(|-| )Founder\y|\yFounder\y|\y(Head|AVP\y|VP\y|EVP\y|SVP\y|Director|President).+(\yIT|Information Technology|Mobility|Product(|s\y))\y|\y(\yIT|Information Technology|Mobility|Product(|s\y)).+(Head|AVP\y|VP\y|EVP\y|SVP\y|Director|President)\y'
and array_length(company_linkedin_url_array,1) = 1)
)a;
