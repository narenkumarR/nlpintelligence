-- creating table

create table ecommerce_companies as 
   select distinct on (a.linkedin_url) a.*,domain,linkedin_name,website_cleaned,
location,region,country
 from linkedin_company_base a join linkedin_company_domains b
   using(linkedin_url) where industry in ('Retail','Apparel & Fashion') and b.domain is not null;
   
create unique index on ecommerce_companies (linkedin_url);


INSERT INTO ecommerce_companies    
SELECT distinct on (a.linkedin_url) a.*,domain,linkedin_name,website_cleaned,
location,region,country
from linkedin_company_base a join linkedin_company_domains b
   using(linkedin_url) where industry in ('Retail','Apparel & Fashion') and b.domain is  null
on conflict do nothing;

INSERT INTO ecommerce_companies    
SELECT distinct on (a.linkedin_url) a.*,domain,linkedin_name,website_cleaned,
location,region,country
from linkedin_company_base a join linkedin_company_domains b
   using(linkedin_url) join tmp_ecommerce_not_dev_descr c using(linkedin_url)
    where industry not in ('Retail','Apparel & Fashion','Information Technology and Services') and b.domain is not null
on conflict do nothing;

INSERT INTO ecommerce_companies    
SELECT distinct on (a.linkedin_url) a.*,domain,linkedin_name,website_cleaned,
location,region,country
from linkedin_company_base a join linkedin_company_domains b
   using(linkedin_url) join tmp_ecommerce_not_dev_descr c using(linkedin_url)
    where industry not in ('Retail','Apparel & Fashion','Information Technology and Services') and b.domain is  null
on conflict do nothing;


---- funding
alter table ecommerce_companies add column funding_rounds text,add column funding_total_usd text,add column first_funding_on text,add column last_funding_on text;

update ecommerce_companies a set funding_rounds=b.funding_rounds,funding_total_usd = b.funding_total_usd,
first_funding_on = b.first_funding_on ,last_funding_on  = b.last_funding_on
from crunchbase_data.organizations b where a.domain=b.domain;

--tech information
alter table ecommerce_companies add column bw_technologies text[];
update ecommerce_companies a set bw_technologies = b.technologies from builtwith_company_technologies b where a.domain=b.domain;

--adding other columns
alter table ecommerce_companies add column channel_presence text, add column marketplace_type text,add column transaction_type text,
	add column product_categories text, add column products_quality text, add column browse_and_filter boolean , 
	add column faceted_search boolean, add column recomm_collab_filter boolean, add column visual_merchandising boolean;

--alexa
alter table ecommerce_companies add column unique_visitors double precision, add column unique_visitors_growth double precision,add column pageviews double precision,add column time_on_site_seconds bigint, add column alexa_global_rank bigint,add column website_language text, add column alexa_category text ;
update ecommerce_companies a set unique_visitors=NULL,unique_visitors_growth=NULL,pageviews=NULL,time_on_site_seconds=NULL,alexa_global_rank=NULL,website_language=NULL,alexa_category=NULL ;

update ecommerce_companies a set unique_visitors=b.unique_visitors,unique_visitors_growth=b.unique_visitors_growth,pageviews=b.pageviews,time_on_site_seconds=b.time_on_site_seconds,alexa_global_rank=b.global_rank,website_language=b.language,alexa_category=b.category from ecommerce_companies_alexa_data b where lower(a.domain)=lower(b.site) and a.domain is not null and b.site is not null;

--only 1576 present

--check how many are in all linkedin_data
create table tmp_alexa_ecom as select distinct on (linkedin_name) b.* from ecommerce_companies_alexa_data a join linkedin_company_domains b on a.site=b.domain;

create table ecommerce_companies_alexa_data_unique as select distinct on (site) * from (select * from ecommerce_companies_alexa_data order by region desc)a;
alter table ecommerce_companies_alexa_data_unique add column category_cleaned text,add column category_cleaned_array text[];

update ecommerce_companies_alexa_data_unique set category_cleaned_array = regexp_split_to_array(category,'-');
update ecommerce_companies_alexa_data_unique set category_cleaned = category_cleaned_array[array_upper(category_cleaned_array, 1)];

update ecommerce_companies_alexa_data_unique set category_cleaned = 'Health_and_Healthcare' where category_cleaned = 'Strength_Sports';

update ecommerce_companies a set alexa_category = b.category_cleaned from ecommerce_companies_alexa_data_unique b where  lower(a.domain)=lower(b.site) and a.domain is not null and b.site is not null;

--builtwith based flags creation
alter table ecommerce_companies add column a_b_testing_present boolean, add column a_b_testing_softwares text[];

