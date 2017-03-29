--update linkedin data
select count(distinct a.domain) from ecommerce.ecommerce_data a join linkedin_company_domains b using(domain)
where sources = array['global_database'] 

--funding
update ecommerce_companies a set funding_rounds=b.funding_rounds,funding_total_usd = b.funding_total_usd,
first_funding_on = b.first_funding_on ,last_funding_on  = b.last_funding_on
from crunchbase_data.organizations b where a.domain=b.domain and a.funding_rounds is null;

--tech information
update ecommerce_companies a set bw_technologies = b.technologies from builtwith_company_technologies b where a.domain=b.domain
and a.bw_technologies is null;

--alexa
update ecommerce_companies a set unique_visitors=b.unique_visitors,unique_visitors_growth=b.unique_visitors_growth,
pageviews=b.pageviews,time_on_site_seconds=b.time_on_site_seconds,alexa_global_rank=b.global_rank,
website_language=b.language,alexa_category=b.category from 
ecommerce_companies_alexa_data b where lower(a.domain)=lower(b.site) and a.domain is not null and b.site is not null and a.unique_visitors is null;

