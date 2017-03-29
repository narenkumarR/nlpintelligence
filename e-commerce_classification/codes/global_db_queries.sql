--sic codes related to retail
--554:gas stations,58:eating places,5994:News Dealers and Newsstands(mostly magazines)
SELECT 
  count(distinct company_website),count(*),count(distinct company_name),count(distinct domain)
FROM 
  global_us_data.statewisedata_extract where sic_code ~ '^5' and sic_code !~ '^5(0|1|54|8|994)|99$'
   limit 100;

select * from global_us_data.statewisedata_extract order by domain desc limit 100
--sample data
SELECT * FROM global_us_data.statewisedata_extract where sic_code ~ '99$' and sic_code ~ '^5' and sic_code !~ '^5(0|1|54|8|994)'-- and naics_code!='451212' 
order by random() limit 100;

--insert into ecommerce table
--adding domain
alter table global_us_data.statewisedata_extract add column domain text;
update global_us_data.statewisedata_extract set domain = get_domain_from_website(lower(company_website));
--create columns
alter table ecommerce.ecommerce_data add column sic_code text;
alter table ecommerce.ecommerce_data add column revenue_global_db text;
--insert data
insert into ecommerce.ecommerce_data (domain,company_name,industry,description,sic_code,headquarters,location,region,country,revenue_global_db,
      company_size,sources)
  select domain,company_name,industry,a.desc,sic_code,address,city,state,'UNITED STATES',revenue,employees,
      array['global_database'] from global_us_data.statewisedata_extract a where sic_code ~ '^5' and sic_code !~ '^5(0|1|54|8|994)|99$' and
    domain not in (select distinct domain from ecommerce.ecommerce_data where domain is not null);


