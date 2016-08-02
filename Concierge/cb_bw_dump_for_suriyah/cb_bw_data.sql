drop table if exists concierge_table_cb_bw_1;
CREATE  TABLE concierge_table_cb_bw_1 as 
SELECT DISTINCT ON (cb_c.domain)
  cb_c.domain, 
  lower(cb_c.company_name) as company_name,
  cb_c.employee_count  as employee_count,
  cb_c.primary_role, 
  cb_c.homepage_url, 
  case when cb_c.country_code != '' then cb_c.country_code else bw_c."Country" end as country, 
  case when cb_c.state_code != '' then cb_c.state_code else bw_c."State" end as state, 
  cb_c.region, 
  case when cb_c.city != '' then cb_c.city else bw_c."City" end as city, 
  bw_c."Zip" as zipcode,
  cb_c.status, 
  cb_c.category_list,
  cb_c.category_group_list,
  bw_c."Vertical" as vertical,
  cb_c.funding_rounds,
  cb_c.funding_total_usd,
  cb_c.short_description as description, 
  cb_c.founded_on  as founded_on, 
  case when cb_c.facebook_url != '' then cb_c.facebook_url else bw_c."Facebook" end as facebook_url, 
  cb_c.cb_url, 
  case when cb_c.twitter_url != '' then cb_c.twitter_url else bw_c."Twitter" end as twitter_url,
  bw_c."Alexa" as alexa, 
  bw_c."LinkedIn" as linkedin_url, 
  cb_c.email, 
  cb_c.phone, 
  bw_c."Telephones" telephones_bw, 
  bw_c."Emails" emails_bw, 
  bw_c."People" people_bw,  
  bw_c_t.techs technologies,
  cb_c.uuid
FROM 
  crunchbase_data.organizations cb_c left join
  public.builtwith_companies_meta_data bw_c on cb_c.domain = bw_c."Domain" left join
  public.builtwith_company_technologies bw_c_t on bw_c."Domain" = bw_c_t.domain
WHERE
  (  
    (cb_c.domain != '' and bw_c."Domain" != '') or (cb_c.domain is null or cb_c.domain = '')
  ) ;

drop table if exists concierge_table_cb_bw_3;
create table concierge_table_cb_bw_3 as
select DISTINCT a.*,b.first_name,'' as middle_name,b.last_name,
  b.primary_affiliation_title as designation,
   b.primary_affiliation_organization as company_person
 from
  concierge_table_cb_bw_1 a join crunchbase_data.people b on
  a.uuid = b.primary_organization_uuid
;