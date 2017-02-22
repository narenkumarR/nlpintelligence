--insideview related tables
set search_path to public;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

set search_path to crawler;
drop table if exists crawler.list_table_insideview_companies;
create table crawler.list_table_insideview_companies
(
    id UUID PRIMARY KEY DEFAULT public.uuid_generate_v1mc(),
    list_name text,
    created_on timestamp default current_timestamp
);
create unique index on crawler.list_table_insideview_companies (list_name);

drop table if exists crawler.list_items_insideview_companies;
create table crawler.list_items_insideview_companies
(
    id UUID PRIMARY KEY DEFAULT public.uuid_generate_v1mc(),
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
create index on crawler.insideview_company_search_res (list_id);

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
create index on crawler.insideview_contact_search_res (company_id);

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
create index on crawler.insideview_contact_data (contact_id);
create index on crawler.insideview_contact_data (company_id);
create index on crawler.insideview_contact_data (email_md5_hash);

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
create index on crawler.insideview_company_details_contact_search (company_id);

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
    id UUID PRIMARY KEY DEFAULT public.uuid_generate_v1mc(),
    list_name text,
    created_on timestamp default current_timestamp
);
create unique index on crawler.list_table_insideview_contacts (list_name);

drop table if exists crawler.list_input_insideview_contacts;
create table crawler.list_input_insideview_contacts
(
    id UUID PRIMARY KEY DEFAULT public.uuid_generate_v1mc(),
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
create index on crawler.insideview_contact_name_search_res (list_id);

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
    news_api_hits integer default 0,
     created_on timestamp default current_timestamp,
     updated_on timestamp default current_timestamp
);

--news related tables
drop table if exists crawler.insideview_news_data;
create table crawler.insideview_news_data (
    id serial primary key,
    company_id integer,
    title text,
    url text,
    publication_date timestamp,
    source text,
    agents text,
    image_url text,
    similar_news_count text,
    created_on timestamp default current_timestamp,
    updated_on timestamp default current_timestamp
);

