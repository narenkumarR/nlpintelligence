

create table crawler.list_table(
    id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    list_name text,
    created_on timestamp default current_timestamp);
create unique index on crawler.list_table (list_name);

create table crawler.list_items(id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    list_id UUID,
    list_input text,
    list_input_additional text,
    created_on timestamp default current_timestamp);
create unique index on crawler.list_items (list_id,list_input,list_input_additional);

create table crawler.list_items_urls(id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    list_id UUID,
    list_items_id UUID,
    url text,
    created_on TIMESTAMP default current_timestamp);
create unique index on crawler.list_items_urls (list_id,list_items_id,url);

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

CREATE TABLE crawler.linkedin_company_urls_to_crawl (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    url text ,
    list_id UUID,
    list_items_url_id UUID,
    created_on timestamp default current_timestamp
);
create unique index on crawler.linkedin_company_urls_to_crawl(url,list_id,list_items_url_id);

CREATE TABLE crawler.linkedin_people_urls_to_crawl (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    url text ,
    list_id UUID,
    list_items_url_id UUID,
    created_on timestamp default current_timestamp
);
create unique index on crawler.linkedin_people_urls_to_crawl(url,list_id,list_items_url_id);

CREATE TABLE crawler.linkedin_company_finished_urls (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    url text ,
    list_id UUID,
    list_items_url_id UUID,
    created_on timestamp default current_timestamp
);
create unique index on crawler.linkedin_company_finished_urls(url,list_id,list_items_url_id);

CREATE TABLE crawler.linkedin_people_finished_urls (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    url text ,
    list_id UUID,
    list_items_url_id UUID,
    created_on timestamp default current_timestamp
);
create unique index on crawler.linkedin_people_finished_urls(url,list_id,list_items_url_id);

CREATE TABLE crawler.linkedin_people_urls_to_crawl_priority (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    url text ,
    list_id UUID,
    list_items_url_id UUID,
    created_on timestamp default current_timestamp
);
create unique index on crawler.linkedin_people_urls_to_crawl_priority(url,list_id,list_items_url_id);

CREATE TABLE crawler.linkedin_company_urls_to_crawl_priority (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    url text ,
    list_id UUID,
    list_items_url_id UUID,
    created_on timestamp default current_timestamp
);
create unique index on crawler.linkedin_company_urls_to_crawl_priority(url,list_id,list_items_url_id);

--capturing url redirection
CREATE TABLE crawler.linkedin_company_redirect_url (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    url text, redirect_url text,
    created_on timestamp default current_timestamp
);
CREATE TABLE crawler.linkedin_people_redirect_url (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    url text, redirect_url text,
    created_on timestamp default current_timestamp
);
create index on crawler.linkedin_company_redirect_url(url,redirect_url);
create index on crawler.linkedin_people_redirect_url(url,redirect_url);


--email table
create table crawler.people_details_for_email_verifier (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    list_id UUID,
    list_items_url_id UUID,
    first_name text,
    middle_name text,
    last_name text,
    domain text,
    created_on timestamp default current_timestamp
);

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

