--create extensions and schema
CREATE SCHEMA IF NOT EXISTS crawler;
set search_path to crawler;
CREATE EXTENSION if not exists "uuid-ossp" schema crawler;
CREATE EXTENSION if not exists pgcrypto;
CREATE EXTENSION plpythonu;

--create tables
drop table if exists crawler.list_table;
create table crawler.list_table(
    id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    list_name text,
    created_on timestamp default current_timestamp);
create unique index on crawler.list_table (list_name);

drop table if exists crawler.list_items;
create table crawler.list_items(id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    list_id UUID,
    list_input text,
    list_input_additional text,
    url_extraction_tried BIGINT,
    created_on timestamp default current_timestamp);
create unique index on crawler.list_items (list_id,list_input,list_input_additional);

drop table if exists crawler.list_items_urls;
create table crawler.list_items_urls(id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    list_id UUID,
    list_items_id UUID,
    url text,
    created_on TIMESTAMP default current_timestamp);
create unique index on crawler.list_items_urls (list_id,list_items_id,url);

drop table if exists crawler.linkedin_people_base;
CREATE TABLE crawler.linkedin_people_base (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    linkedin_url text ,
    name text,
    sub_text text,
    location text,
    company_name text,
    company_linkedin_url text,
    previous_companies text,
    education text,
    industry text,
    summary text,
    skills text,
    experience text,
    related_people text,
    same_name_people text,
    list_id UUID,
    list_items_url_id UUID,
    created_on timestamp default current_timestamp
);
create index on crawler.linkedin_people_base (linkedin_url);

drop table if exists crawler.linkedin_company_base;
CREATE TABLE crawler.linkedin_company_base (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    linkedin_url text ,
    company_name text,
    company_size text,
    industry text,
    company_type text,
    headquarters text,
    description text,
    founded text,
    specialties text,
    website text,
    employee_details text,
    also_viewed_companies text,
    list_id UUID,
    list_items_url_id UUID,
    created_on timestamp default current_timestamp
);
create index on crawler.linkedin_company_base (linkedin_url);

drop table if exists crawler.linkedin_company_urls_to_crawl;
CREATE TABLE crawler.linkedin_company_urls_to_crawl (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    url text ,
    list_id UUID,
    list_items_url_id UUID,
    created_on timestamp default current_timestamp
);
create unique index on crawler.linkedin_company_urls_to_crawl(url,list_id,list_items_url_id);

drop table if exists crawler.linkedin_people_urls_to_crawl;
CREATE TABLE crawler.linkedin_people_urls_to_crawl (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    url text ,
    list_id UUID,
    list_items_url_id UUID,
    created_on timestamp default current_timestamp
);
create unique index on crawler.linkedin_people_urls_to_crawl(url,list_id,list_items_url_id);

drop table if exists crawler.linkedin_company_finished_urls;
CREATE TABLE crawler.linkedin_company_finished_urls (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    url text ,
    list_id UUID,
    list_items_url_id UUID,
    created_on timestamp default current_timestamp
);
create unique index on crawler.linkedin_company_finished_urls(url,list_id,list_items_url_id);

drop table if exists crawler.linkedin_people_finished_urls;
CREATE TABLE crawler.linkedin_people_finished_urls (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    url text ,
    list_id UUID,
    list_items_url_id UUID,
    created_on timestamp default current_timestamp
);
create unique index on crawler.linkedin_people_finished_urls(url,list_id,list_items_url_id);

drop table if exists crawler.linkedin_people_urls_to_crawl_priority;
CREATE TABLE crawler.linkedin_people_urls_to_crawl_priority (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    url text ,
    list_id UUID,
    list_items_url_id UUID,
    created_on timestamp default current_timestamp
);
create unique index on crawler.linkedin_people_urls_to_crawl_priority(url,list_id,list_items_url_id);

drop table if exists crawler.linkedin_company_urls_to_crawl_priority;
CREATE TABLE crawler.linkedin_company_urls_to_crawl_priority (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    url text ,
    list_id UUID,
    list_items_url_id UUID,
    created_on timestamp default current_timestamp
);
create unique index on crawler.linkedin_company_urls_to_crawl_priority(url,list_id,list_items_url_id);

--capturing url redirection
drop table if exists crawler.linkedin_company_redirect_url;
CREATE TABLE crawler.linkedin_company_redirect_url (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    url text, redirect_url text,
    created_on timestamp default current_timestamp
);
drop table if exists crawler.linkedin_people_redirect_url;
CREATE TABLE crawler.linkedin_people_redirect_url (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    url text, redirect_url text,
    created_on timestamp default current_timestamp
);
create index on crawler.linkedin_company_redirect_url(url,redirect_url);
create index on crawler.linkedin_people_redirect_url(url,redirect_url);


--email table
drop table if exists crawler.people_details_for_email_verifier_new;
create table crawler.people_details_for_email_verifier_new (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    list_id UUID,
    list_items_url_id UUID,
    full_name text,
    first_name text,
    middle_name text,
    last_name text,
    domain text,
    designation text,
    company_name text,
    company_website text,
    headquarters  text,
    location_person text,    
    industry text,
    company_size text,
    founded text,
    company_linkedin_url text,
    people_linkedin_url text, 
    created_on timestamp default current_timestamp
);
alter table crawler.people_details_for_email_verifier_new add primary key (id);
create unique index on crawler.people_details_for_email_verifier_new (list_id,full_name,domain,designation);

--functions
CREATE OR REPLACE FUNCTION extract_related_info(in_array text[],look_value integer)
RETURNS text[]
AS
$$
DECLARE
   retVal text[];
BEGIN
    IF in_array != '{}' THEN
       FOR I IN array_lower(in_array, 1)..array_upper(in_array, 1) LOOP
            retVal[I] := split_part(in_array[I],'{}',look_value);
       END LOOP;
    END IF;
RETURN retVal;
END;
$$
LANGUAGE plpgsql
   STABLE
RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION clean_linkedin_url_array(text[])
RETURNS text[]
AS
$$
DECLARE
   urlTexts ALIAS FOR $1;
   retVal text[];
BEGIN
   FOR I IN array_lower(urlTexts, 1)..array_upper(urlTexts, 1) LOOP
    retVal[I] := split_part(urlTexts[I],'?trk',1);
   END LOOP;
RETURN retVal;
END;
$$
LANGUAGE plpgsql
   STABLE
RETURNS NULL ON NULL INPUT;

--name cleaner
CREATE OR REPLACE FUNCTION name_cleaner(name text) RETURNS text[]
LANGUAGE plpythonu
AS $$
if 'name_tools' in SD:
    name_tools = SD['name_tools']
else:
    import name_tools
    SD['name_tools'] = name_tools
if 're' in SD:
    re = SD['re']
else:
    import re
    SD['re'] = re
name1 = name.split(',')[0]
name1 = re.sub('[!@#$%^&*()}{></~+_]|\[|\]',' ',name1)
name1 = re.sub(' +',' ',name1)
name_cleaned = name_tools.split(name1)
f_part = name_cleaned[1].split()
if len(f_part) == 1:
    f_name,m_name = f_part[0],''
elif len(f_part) > 1:
    f_name,m_name = f_part[0],' '.join(f_part[1:])
else:
    f_name,m_name = '',''
name_list = [name_cleaned[0],f_name,m_name,name_cleaned[2],name_cleaned[3]]
return name_list
$$;


drop table if exists crawler.linkedin_company_base_login;
CREATE TABLE crawler.linkedin_company_base_login (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    linkedin_url text ,
    company_name text,
    company_size text,
    industry text,
    company_type text,
    headquarters text,
    description text,
    founded text,
    specialties text,
    website text,
    employee_details text,
    also_viewed_companies text,
    employee_count_linkedin text,
    list_id UUID,
    list_items_url_id UUID,
    created_on timestamp default current_timestamp
);
create index on crawler.linkedin_company_base_login (linkedin_url);

drop table if exists crawler.linkedin_people_base_login;
CREATE TABLE crawler.linkedin_people_base_login (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    linkedin_url text ,
    name text,
    sub_text text,
    location text,
    company_name text,
    company_linkedin_url text,
    previous_companies text,
    education text,
    industry text,
    summary text,
    skills text,
    experience text,
    related_people text,
    same_name_people text,
    list_id UUID,
    list_items_url_id UUID,
    created_on timestamp default current_timestamp
);
create index on crawler.linkedin_people_base_login (linkedin_url);

--insideview related tables
drop table if exists crawler.list_table_insideview_companies;
create table crawler.list_table_insideview_companies
(
    id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    list_name text,
    created_on timestamp default current_timestamp
);
create unique index on crawler.list_table_insideview_companies (list_name);

drop table if exists crawler.list_items_insideview_companies;
create table crawler.list_items_insideview_companies
(
    id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    list_id UUID,
    company_name text,
    website text,
    country text,
    state text,
    city text,
    created_on timestamp default current_timestamp
);
create unique index on crawler.list_items_insideview_companies (list_id,company_name,website);

drop table if exists crawler.insideview_company_search_res;
create table crawler.insideview_company_search_res
   (id serial primary key,
    list_id UUID,
    list_items_id UUID,
    city text,
    state text,
    country text,
    name text,
    company_id integer,
    created_on timestamp default current_timestamp,
    updated_on timestamp default current_timestamp);

drop table if exists crawler.insideview_contact_search_res;
create table crawler.insideview_contact_search_res
   (id serial primary key,
    list_id UUID,
    first_name text,
    last_name text,
    full_name text,
    new_contact_id text,
    people_id text,
    titles text[],
    active boolean,
    company_id integer,
    company_name text,
    has_email boolean,
    email_md5_hash text,
    has_phone boolean,
    city text,
    state text,
    country text,
    created_on timestamp default current_timestamp,
    updated_on timestamp default current_timestamp);
create unique index on crawler.insideview_contact_search_res (list_id,new_contact_id);

drop table if exists crawler.insideview_contact_data;
create table crawler.insideview_contact_data
   (id serial primary key,
    contact_id integer,
    people_id text,
    active boolean,
    first_name text,
    last_name text,
    full_name text,
    titles text[],
    company_id integer,
    company_name text,
    age integer,
    description text,
    email text,
    email_md5_hash text,
    job_function text[],
    job_levels text[],
    phone text,
    salary text,
    salary_currency text,
    image_url text,
    facebook_url text,
    linkedin_url text,
    twitter_url text,
    sources text[],
    created_on timestamp default current_timestamp,
    updated_on timestamp default current_timestamp);

drop table if exists crawler.insideview_contact_education;
create table crawler.insideview_contact_education
   (id serial primary key,
    contact_id integer,
    people_id text,
    degree text,
    major text,
    university text,
    created_on timestamp default current_timestamp,
    updated_on timestamp default current_timestamp);


drop table if exists crawler.insideview_company_details_contact_search;
create table crawler.insideview_company_details_contact_search
   (id serial primary key,
    company_id integer,
    company_status text,
    company_type text,
    name text,
    websites text[],
    subsidiary text,
    parent_company_id text,
    parent_company_name text,
    parent_company_country text,
    industry text,
    industry_code text,
    sub_industry text,
    sub_industry_code text,
    sic text,
    sic_description text,
    naics text,
    naics_description text,
    employees text,
    employee_range text,
    fortune_ranking text,
    foundation_date text,
    gender text,
    ethnicity text,
    dbe text,
    wbe text,
    mbe text,
    vbe text,
    disabled text,
    lgbt text,
    revenue text,
    revenue_currency text,
    revenue_range text,
    most_recent_quarter text,
    financial_year_end text,
    phone text,
    fax text,
    street text,
    city text,
    state text,
    country text,
    zip text,
    equifax_id text,
    ultimate_parent_company_id text,
    ultimate_parent_company_name text,
    ultimate_parent_company_country text,
    sources text[],
    created_on timestamp default current_timestamp,
    updated_on timestamp default current_timestamp);

drop table if exists crawler.insideview_company_british_sics;
create table crawler.insideview_company_british_sics
    (
        id serial primary key,
        company_id integer,
        british_sic text,
        description text,
        created_on timestamp default current_timestamp,
        updated_on timestamp default current_timestamp
    );

drop table if exists crawler.insideview_company_tickers;
create table crawler.insideview_company_tickers
    (
        id serial primary key,
        company_id integer,
        ticker_name text,
        exchange text,
        created_on timestamp default current_timestamp,
        updated_on timestamp default current_timestamp
    );

drop table if exists crawler.insideview_company_tech_details;
create table crawler.insideview_company_tech_details
   (
   id serial primary key,
   company_id integer,
   category_id text,
   category_name text,
   sub_category_id text,
   sub_category_name text,
   product_id text,
   product_name text,
   created_on timestamp default current_timestamp,
   updated_on timestamp default current_timestamp
   );

--contact search related tables
drop table if exists crawler.list_table_insideview_contacts;
create table crawler.list_table_insideview_contacts
(
    id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    list_name text,
    created_on timestamp default current_timestamp
);
create unique index on crawler.list_table_insideview_contacts (list_name);

drop table if exists crawler.list_input_insideview_contacts;
create table crawler.list_input_insideview_contacts
(
    id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    list_id UUID,
    input_filters json,
    created_on timestamp default current_timestamp
);
--create unique index on crawler.list_input_insideview_contacts (list_id,input_filters);

--this table contains search result for name and company
--input_name_id tracks for which input details we got this search result--mapped from new_contact_id in
-- insideview_contact_search_res table or id in id in list_input_insideview_contacts
drop table if exists crawler.insideview_contact_name_search_res;
create table crawler.insideview_contact_name_search_res
   (id serial primary key,
    list_id UUID,
    list_items_id UUID,
    first_name text,
    middle_name text,
    last_name text,
    contact_id integer,
    company_id integer,
    company_name text,
    titles text[],
    active boolean,
    has_email boolean,
    has_phone boolean,
    people_id text,
    input_name_id text,
    created_on timestamp default current_timestamp,
    updated_on timestamp default current_timestamp);

-- api hit tracker
drop table if exists crawler.insideview_api_hits;
create table crawler.insideview_api_hits (
    id serial primary key,
    list_id UUID,
    company_search_hits integer default 0,
    company_details_hits integer default 0,
    newcontact_search_hits integer default 0,
    newcontact_email_hits integer default 0,
    people_search_hits integer default 0,
    contact_fetch_hits integer default 0,
     created_on timestamp default current_timestamp,
     updated_on timestamp default current_timestamp
);