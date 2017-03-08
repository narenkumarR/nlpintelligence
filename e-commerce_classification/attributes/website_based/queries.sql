select distinct on(website) a.website,misc_details ->> 'seperate_shipping_page_present' as shipping_page_present,misc_details ->> 'shipping_present_in_page' as shipping_text_present,misc_details ->> 'shipping_providers' as shipping_providers 
from rakutan_websites_bck a join webpage_texts b on a.website=b.domain 

select domain,string_agg(keys,'|') keys1 from (
select domain,json_object_keys(all_page_text) :: text as keys from (select domain,all_page_text from public.webpage_texts limit 100)x
)y
 group by domain;

select domain from public.webpage_texts 
where all_page_text::text ~* '(((Bought|viewed) this Item (.){,20}(Bought|viewed)|Recommendations(.){,30}history|(may|might) also be interested).*){1,}'
limit 100;
"abebooks.com"
"activeherb.com"

select domain,all_page_text::text from webpage_texts where domain in ('metriccoffee.com'),'activeherb.com')

select domain from public.webpage_texts 
where all_page_text::text ~* '(look|search)(.){,20}similar (products|items)|'
limit 100;


select domain from ecommerce.ecommerce_data where bw_technologies is null and alexa_category is not null and linkedin_url is null limit 100;