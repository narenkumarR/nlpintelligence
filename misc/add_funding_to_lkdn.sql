create table tmp_saas_not_dev_funding as select distinct on ( b.linkedin_url,e.funded_at) b.*,c.domain,d.funding_rounds,d.funding_total_usd,e.funded_at AS last_funded_at,e.funding_round_type AS funding_level,e.funding_round_code  from tmp_saas_not_dev a join linkedin_company_base b using(linkedin_url) join linkedin_company_domains c using(linkedin_url) left join  cb_objects d on c.domain = d.domain left join cb_funding_rounds e on d.id = e.object_id where c.domain is not null and c.domain!='' and c.domain != 'NULL' and b.website != 'NULL' order by b.linkedin_url,e.funded_at desc ;


