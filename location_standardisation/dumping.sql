INSERT INTO `delivery_zip`
(`postal_code`, `country_code`, `place_name`, `admin_name1`, `code_name1`, `admin_name2`, `code_name2`, `admin_name3`,
`code_name3`, `latitude`, `longitude`, `accuracy`)
VALUES ('ZA', '9966', 'Smithfield', '0', '0', '0', '0', '0', '0', '-30.2167', '26.5333', 4);

set search_path to location_tabs ;

create table delivery_zip (
    postal_code text,country_code text,place_name text,admin_name1 text,code_name1 text,
    admin_name2 text, code_name2 text, admin_name3 text,code_name3 text, latitude text,longitude text,accuracy int
);


-------using the dump from unece 
SELECT 
  a.country_code, 
  a.region_code, 
  a.location_code,
  a.location_cleaned,
  c.country_name, 
  b.region,
  b.region_code
FROM
  location_tabs.location_details_chk a left join  
  location_tabs.regions_chk b on a.country_code=b.country_code and 
      (a.region_code=b.region_code or a.location_code=b.region_code)left join
  location_tabs.countries_chk c on a.country_code=c.country_code
  where a.country_code = 'US'
   limit 1000

select * from location_tabs.regions_chk limit 100
select * from location_tabs.location_details_chk limit 100
create table location_ref_table (
	id UUID PRIMARY KEY DEFAULT crawler.uuid_generate_v1mc(),
	country_code text,
	region_code text,
	location_code text,
	location text,
	location_cleaned text,
	region text,
	country text);

insert into location_ref_table (country_code,region_code,location_code,location,location_cleaned,region,country)
select distinct a.country_code, 
  a.region_code, 
  a.location_code,
  lower(a.location) as location,
  lower(a.location_cleaned) as  location_cleaned,
  lower(b.region) as  region,
  lower(c.country_name) as country
FROM
  location_tabs.location_details_chk a left join  
  location_tabs.regions_chk b on a.country_code=b.country_code and 
      (a.region_code=b.region_code or a.location_code=b.region_code)left join
  location_tabs.countries_chk c on a.country_code=c.country_code;

select * from location_ref_table limit 100

select a.headquarters,b.location_cleaned,b.region,b.country from public.linkedin_company_base_2016_07_15 a left join location_ref_table b 
on
lower(a.headquarters) like '%' || b.country || '%' and lower(a.headquarters) like '% ' || b.region || ' %' 
   and lower(a.headquarters) like '% ' || b.location_cleaned || ' %'
 limit 100

select a.headquarters,b.location_cleaned,b.region,b.country from public.linkedin_company_base_2016_07_15 a left join location_ref_table b 
on
a.headquarters ~* '\y'||b.country||'\y'  and lower(a.headquarters) like '% ' || b.region || ' %' 
   and lower(a.headquarters) like '% ' || b.location_cleaned || ' %'
 limit 100

---ref tables for locations (with regex for matching)
-- a logical problem here. eg: region paris,location paris or other loc. this method can match location paris with region paris. can give error. need to fix
drop table if exists locations_ref_table_regex;
create table locations_ref_table_regex 
(
	id UUID PRIMARY KEY DEFAULT crawler.uuid_generate_v1mc(),
	country_code text,
	region_code text,
	location_code text,
	location text,
	location_cleaned text,
	region text,
	country text,
	location_reg text,
	region_reg text,
	country_reg text);

insert into locations_ref_table_regex (country_code,region_code,location_code,location,location_cleaned,region,country,
                      location_reg,region_reg,country_reg)
select distinct
  a.country_code,
  a.region_code, 
  a.location_code,
  a.location,
  a.location_cleaned,
  b.region,
  c.country_name,
  '\y'||regexp_replace(a.location_code,'[^a-zA-Z0-9 ]','','g')||'\y|\y'||regexp_replace(a.location_cleaned,'[^a-zA-Z0-9 ]','','g')||'\y' ,
  '\y'||regexp_replace(a.region_code,'[^a-zA-Z0-9 ]','','g')||'\y|\y'||regexp_replace(b.region,'[^a-zA-Z0-9 ]','','g')||'\y', 
  '\y'||regexp_replace(a.country_code,'[^a-zA-Z0-9 ]','','g')||'\y|\y'||regexp_replace(c.country_name,'[^a-zA-Z0-9 ]','','g')||'\y' 

FROM
  location_tabs.location_details_chk a 
  join
  location_tabs.countries_chk c on a.country_code=c.country_code
  left join  
  location_tabs.regions_chk b on a.country_code=b.country_code and 
      (a.region_code=b.region_code)  ;

create index on locations_ref_table_regex (location_reg);
create index on locations_ref_table_regex (region_reg);
create index on locations_ref_table_regex (country_reg);

--create table for regions
drop table if exists regions_ref_table_regex;
create table regions_ref_table_regex 
(	id UUID PRIMARY KEY DEFAULT crawler.uuid_generate_v1mc(),
	country_code text,
	region_code text,
	region text,
	country text,
	region_reg text,
	country_reg text);

insert into regions_ref_table_regex (country_code,region_code,region,country,
                      region_reg,country_reg)
select distinct
  c.country_code,
  b.region_code, 
  b.region,
  c.country_name,
  '\y'||regexp_replace(b.region_code,'[^a-zA-Z0-9 ]','','g')||'\y|\y'||regexp_replace(b.region,'[^a-zA-Z0-9 ]','','g')||'\y', 
  '\y'||regexp_replace(c.country_code,'[^a-zA-Z0-9 ]','','g')||'\y|\y'||regexp_replace(c.country_name,'[^a-zA-Z0-9 ]','','g')||'\y' 
FROM
  location_tabs.regions_chk b  join
  location_tabs.countries_chk c on b.country_code=c.country_code;

create index on regions_ref_table_regex (region_reg);
create index on regions_ref_table_regex (country_reg);

--create table for countries
drop table if exists countries_ref_table_regex;
create table countries_ref_table_regex 
(
	id UUID PRIMARY KEY DEFAULT crawler.uuid_generate_v1mc(),
	country_code text,
	country text,
	country_reg text);
--country reg uses only country name(not country code) in this case
insert into countries_ref_table_regex (country_code,country,country_reg)
select distinct
  c.country_code,
  c.country_name,
  '\y'||regexp_replace(c.country_name,'[^a-zA-Z0-9 ]','','g')||'\y' 
FROM
  location_tabs.countries_chk c ;
create index on countries_ref_table_regex (country_reg);


---checking
--select a.headquarters,b.location_cleaned,b.region,b.country from public.linkedin_company_base_2016_07_15 a left join location_ref_table_regex b 
--on
--(regexp_replace(a.headquarters,'[^a-zA-Z0-9 ]','','g') ~* b.country_reg or regexp_replace(a.headquarters,'[^a-zA-Z0-9 ]','','g') ~ b.country_code_reg ) and 
--   (regexp_replace(headquarters,'[^a-zA-Z0-9 ]','','g') ~* b.region_reg or regexp_replace(headquarters,'[^a-zA-Z0-9 ]','','g') ~ b.region_code_reg) and 
--   (regexp_replace(headquarters,'[^a-zA-Z0-9 ]','','g') ~* b.location_cleaned_reg or regexp_replace(headquarters,'[^a-zA-Z0-9 ]','','g') ~ b.location_code_reg)
-- limit 100;


--testing
drop table if exists tmp_table;
create table tmp_table as select distinct headquarters from public.linkedin_company_base_2016_07_15 limit 10000;
alter table tmp_table add column location text,add column region text, add column country text;
--updating the table
update tmp_table a set location = b.location_cleaned,region=b.region,country=b.country from locations_ref_table_regex b 
where 
(headquarters ~* b.country_reg ) and 
   (headquarters ~* b.region_reg ) and 
   (headquarters ~* b.location_reg);

--updating the table where location is not set with region
update tmp_table a set region=b.region,country=b.country from regions_ref_table_regex b 
where a.location is null and 
(regexp_replace(a.headquarters,'[^a-zA-Z0-9 ]','','g') ~* b.country_reg ) and 
   (regexp_replace(headquarters,'[^a-zA-Z0-9 ]','','g') ~* b.region_reg ) ;


--updating the table where location is not set with countries
update tmp_table a set country=b.country from countries_ref_table_regex b 
where a.location is null and a.region is null and 
(regexp_replace(a.headquarters,'[^a-zA-Z0-9 ]','','g') ~* b.country_reg  )  ;



---in aws
alter table linkedin_company_domains add column location text,add column region text, add column country text;--updating the table
--updating the table
update linkedin_company_domains a set location = b.location_cleaned,region=b.region,country=b.country 
	from linkedin_company_base c , location_tabs.locations_ref_table_regex b 
	where a.linkedin_url = c.linkedin_url and 
	(regexp_replace(headquarters,'[^a-zA-Z0-9 ]','','g') ~* b.country_reg ) and 
   (regexp_replace(headquarters,'[^a-zA-Z0-9 ]','','g') ~* b.region_reg ) and 
   (regexp_replace(headquarters,'[^a-zA-Z0-9 ]','','g') ~* b.location_reg);


--updating the table where location is not set with region
update linkedin_company_domains a set region=b.region,country=b.country 
	from linkedin_company_base c , location_tabs.regions_ref_table_regex b 
	where a.linkedin_url = c.linkedin_url and 
 a.location is null and 
(regexp_replace(headquarters,'[^a-zA-Z0-9 ]','','g') ~* b.country_reg ) and 
   (regexp_replace(headquarters,'[^a-zA-Z0-9 ]','','g') ~* b.region_reg ) ;


--updating the table where location is not set with countries
update linkedin_company_domains a set country=b.country 
	from linkedin_company_base c , countries_ref_table_regex b 
where a.location is null and a.region is null and a.linkedin_url = c.linkedin_url and
(regexp_replace(headquarters,'[^a-zA-Z0-9 ]','','g') ~* b.country_reg  )  ;

--need to improve speed
alter table locations_ref_table_regex add column final_reg text;
update locations_ref_table_regex set final_reg = '(' || location_reg || ').*(' || region_reg || ').*(' || country_reg || ')' where region_reg is not null;
update locations_ref_table_regex set final_reg = '(' || location_reg || ').*(' || country_reg || ')' where region_reg is null;

--testing.. this seems very slow for some reason
drop table if exists tmp_table;
create table tmp_table as select distinct headquarters from public.linkedin_company_base_2016_07_15 limit 1000;
update tmp_table set headquarters = regexp_replace(headquarters,'[^a-zA-Z0-9 ]','','g') ;
alter table tmp_table add column location text,add column region text, add column country text;

update tmp_table a set location = b.location_cleaned,region=b.region,country=b.country from locations_ref_table_regex b 
where a.headquarters ~* b.final_reg;

--using simple like function
alter table countries_ref_table_regex add column country_like text,add column country_code_like text;
update countries_ref_table_regex set 
        country_like = '%' ||  trim(lower(regexp_replace(country,'[^a-zA-Z0-9 ]','','g'))) || '%',
        country_code_like = '% ' || trim(regexp_replace(country_code,'[^a-zA-Z0-9 ]','','g')) || ' %';
alter table regions_ref_table_regex add column region_like text,add column region_code_like text,add column country_like text,add column country_code_like text;
update regions_ref_table_regex set 
        region_like = '% ' || trim(lower(regexp_replace(region,'[^a-zA-Z0-9 ]','','g'))) || ' %',
        region_code_like = '% ' || trim(regexp_replace(region_code,'[^a-zA-Z0-9 ]','','g')) || ' %',
        country_like = '%' ||  trim(lower(regexp_replace(country,'[^a-zA-Z0-9 ]','','g'))) || '%',
        country_code_like = '% ' || trim(regexp_replace(country_code,'[^a-zA-Z0-9 ]','','g')) || ' %';


update tmp_table a set country=b.country from countries_ref_table_regex b 
  where (headquarters like country_code_like or lower(headquarters) like country_like);

update tmp_table a set region=b.region,country=b.country from regions_ref_table_regex b 
  where (headquarters like region_code_like or lower(headquarters) like region_like) and 
        (headquarters like country_code_like or lower(headquarters) like country_like);


alter table locations_ref_table_regex add column location_like text,add column location_code_like text,
          add column region_like text,add column region_code_like text,add column country_like text,add column country_code_like text;
update locations_ref_table_regex set 
        location_like = '% ' || trim(lower(regexp_replace(location_cleaned,'[^a-zA-Z0-9 ]','','g'))) || ' %',
        location_code_like = '% ' || trim(regexp_replace(location_code,'[^a-zA-Z0-9 ]','','g')) || ' %',
        region_like = '% ' || trim(lower(regexp_replace(region,'[^a-zA-Z0-9 ]','','g'))) || ' %',
        region_code_like = '% ' || trim(regexp_replace(region_code,'[^a-zA-Z0-9 ]','','g')) || ' %',
        country_like = '%' ||  trim(lower(regexp_replace(country,'[^a-zA-Z0-9 ]','','g'))) || '%',
        country_code_like = '% ' || trim(regexp_replace(country_code,'[^a-zA-Z0-9 ]','','g')) || ' %';

update tmp_table a set location = b.location_cleaned,region=b.region,country=b.country from locations_ref_table_regex b 
  where (headquarters like location_code_like or lower(headquarters) like location_like) and 
        (headquarters like region_code_like or lower(headquarters) like region_like) and 
        (headquarters like country_code_like or lower(headquarters) like country_like);


--in server
update public.linkedin_company_domains a set country=b.country from countries_ref_table_regex b 
  where (headquarters like country_code_like or lower(headquarters) like country_like);

update public.linkedin_company_domains a set region=b.region,country=b.country from regions_ref_table_regex b 
  where (headquarters like region_code_like or lower(headquarters) like region_like) and 
        (headquarters like country_code_like or lower(headquarters) like country_like);
update public.linkedin_company_domains a set location = b.location_cleaned,region=b.region,country=b.country from locations_ref_table_regex b 
  where (headquarters like location_code_like or lower(headquarters) like location_like) and 
        (headquarters like region_code_like or lower(headquarters) like region_like) and 
        (headquarters like country_code_like or lower(headquarters) like country_like);

