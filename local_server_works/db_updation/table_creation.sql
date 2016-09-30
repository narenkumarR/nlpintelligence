
CREATE EXTENSION if not exists "uuid-ossp";
CREATE EXTENSION if not exists pgcrypto;
CREATE EXTENSION plpythonu;

CREATE TABLE linkedin_people_base (
    linkedin_url text ,
    name text,
    sub_text text,
    location text,
    company_name text,
    previous_companies text,
    education text,
    industry text,
    summary text,
    skills text,
    timestamp timestamp default current_timestamp,
    company_linkedin_url_array text[],
    related_people_array text[],
    same_name_people_array text[],
    experience_array text[]

);
CREATE TABLE linkedin_company_base (
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
    timestamp timestamp default current_timestamp,
    employee_details_array text[],
    also_viewed_companies_array text[]
);

CREATE TABLE people_company_mapper (
    people_url text,
    company_url text
);
create unique index on people_company_mapper (company_url,people_url);
create index on people_company_mapper (people_url);

CREATE TABLE company_urls_mapper (
    base_url text,
    alias_url text
);
create unique index on company_urls_mapper (base_url,alias_url);
create index on company_urls_mapper (alias_url);

CREATE TABLE people_urls_mapper (
    base_url text,
    alias_url text
);
create unique index on people_urls_mapper (base_url,alias_url);
create index on people_urls_mapper (alias_url);

CREATE TABLE builtwith_linkedin_mapper (
    url_linkedin_table text,
    domain_builtwith_table text
);
create unique index on builtwith_linkedin_mapper (url_linkedin_table,domain_builtwith_table);
create index on builtwith_linkedin_mapper (domain_builtwith_table);

CREATE TABLE crunchbase_linkedin_mapper (
    url_linkedin_table text,
    homepage_url_crunchbase_table text
);
create unique index on crunchbase_linkedin_mapper (url_linkedin_table,homepage_url_crunchbase_table);
create index on crunchbase_linkedin_mapper (homepage_url_crunchbase_table);


##############################################################################
linkedin completed
-t linkedin_company_base_2016_06_07 -t linkedin_company_base_2016_06_08 -t linkedin_company_base_2016_06_10 -t linkedin_people_base_2016_07_15 -t linkedin_people_base_for_investor -t linkedin_people_base_2016_06_15 -f linkedin_tables_all_dump_test.sql
################################################################################

