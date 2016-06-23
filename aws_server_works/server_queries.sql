CREATE TABLE linkedin_people_base (
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
    timestamp timestamp default current_timestamp
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
    employee_details text,
    also_viewed_companies text,
    timestamp timestamp default current_timestamp
);

#inserting into people table - do not include timestamp field if it is not present in the table
#if timestamp present in table
insert into linkedin_people_base (linkedin_url,name,sub_text,
    location,company_name,company_linkedin_url,previous_companies,
    education,industry,summary,skills,experience ,related_people,
    same_name_people,timestamp ) 
     select linkedin_url,name,sub_text,
    location,company_name,company_linkedin_url,previous_companies,
    education,industry,summary,skills,experience ,related_people,
    same_name_people,timestamp from linkedin_people_base_date;

#inserting into company table
insert into linkedin_company_base (linkedin_url,company_name ,
    company_size,industry,company_type,headquarters,description ,
    founded,specialties,website,employee_details,also_viewed_companies ,
    timestamp) 
    select linkedin_url,company_name ,
    company_size,industry,company_type,headquarters,description ,
    founded,specialties,website,employee_details,also_viewed_companies ,
    timestamp from linkedin_company_base_date;


####creating new columns
alter table linkedin_company_base add column employee_details_array text[];
alter table linkedin_company_base add column also_viewed_companies_array text[];
update linkedin_company_base set employee_details_array = clean_linkedin_url_array(string_to_array(replace(employee_details,'NULL',''),','))
    where employee_details like '%linkedin%';
update linkedin_company_base set employee_details = NULL;
alter table linkedin_company_base drop column employee_details;
update linkedin_company_base set also_viewed_companies_array = clean_linkedin_url_array(string_to_array(replace(also_viewed_companies,'NULL',''),','))
    where also_viewed_companies like '%linkedin%';
update linkedin_company_base set also_viewed_companies = NULL;
alter table linkedin_company_base drop column also_viewed_companies;

alter table linkedin_people_base add column company_linkedin_url_array text[];
alter table linkedin_people_base add column related_people_array text[];
alter table linkedin_people_base add column same_name_people_array text[];
update linkedin_people_base set company_linkedin_url_array = clean_linkedin_url_array(string_to_array(replace(company_linkedin_url,'NULL',''),','))
    where company_linkedin_url like '%linkedin%';
alter table linkedin_people_base drop column company_linkedin_url;
update linkedin_people_base set related_people_array = clean_linkedin_url_array(string_to_array(replace(related_people,'NULL',''),','))
    where related_people like '%linkedin%';
alter table linkedin_people_base drop column related_people;
update linkedin_people_base set same_name_people_array = clean_linkedin_url_array(string_to_array(replace(same_name_people,'NULL',''),','))
    where same_name_people like '%linkedin%';
alter table linkedin_people_base drop column same_name_people;
#####

##cleaning fields
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

###
###extracting info from new related fields
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

#####linkedin url similarity measurement ## not implemented
CREATE OR REPLACE FUNCTION linkedin_url_similarity(url1 text,url2 text, min_distance integer) RETURNS integer
LANGUAGE plpythonu
AS $$
if 're' in SD:
    re = SD['re']
else:
    import re
    SD['re'] = re
if 'Levenshtein' in SD:
    Levenshtein = SD['Levenshtein']
else:
    import Levenshtein
    SD['Levenshtein'] = Levenshtein
ret_prob = 0
uname1_split = re.split('linkedin.com/',url1)
uname2_split = re.split('linkedin.com/',url2)
if len(uname1_split) > 1 and len(uname2_split) > 1:
    uname1 = uname1_split[1]
    uname2 = uname2_split[1]
    uname1_l = uname1.split('/')
    uname2_l = uname2.split('/')
    if len(uname1_l) > 1:
        if uname1_l[0] in ['in','pub']:
            uname1_l = uname1_l[1:]
    if len(uname2_l) > 1:
        if uname2_l[0] in ['in','pub']:
            uname2_l = uname2_l[1:]
    if len(uname1_l) > 1:
        uname1_text = uname1_l[0] + ''.join(uname1_l[1:][::-1])
    elif len(uname1_l) == 1:
        uname1_text = uname1_l[0]
    else:
        uname1_text = ''
    if len(uname2_l) > 1:
        uname2_text = uname2_l[0] + ''.join(uname2_l[1:][::-1])
    elif len(uname2_l) == 1:
        uname2_text = uname2_l[0]
    else:
        uname2_text = ''
    uname1_text = re.sub('[^a-zA-Z0-9]','',uname1_text)
    uname2_text = re.sub('[^a-zA-Z0-9]','',uname2_text)
    if uname1_text and uname2_text:
        if Levenshtein.distance(uname1_text,uname2_text) < min_distance:
            ret_prob = 1
return ret_prob
$$;

###linkedin username extraction from url
CREATE OR REPLACE FUNCTION linkedin_username_extraction(url text) RETURNS text
LANGUAGE plpythonu
AS
$$
if 're' in SD:
    re = SD['re']
else:
    import re
    SD['re'] = re
uname_split = re.split('linkedin.com/',url)
if len(uname_split) > 1:
    uname = uname_split[1]
    uname_l = uname.split('/')
    if len(uname_l) > 1:
        if uname_l[0] in ['in','pub']:
            uname_l = uname_l[1:]
        if uname_l[-1] in ['en','es']:
            uname_l = uname_l[:-1]
    if len(uname_l) > 1:
        uname_text = uname_l[0] + ''.join(uname_l[1:][::-1])
    elif len(uname_l) == 1:
        uname_text = uname_l[0]
    else:
        uname_text = ''
else:
    uname_text = uname_split[0]
uname_text = re.sub('[^a-zA-Z0-9]','',uname_text)
return uname_text
$$;

####url correction
--COMPANY TO PEOPLE URLS FROM COMPANY TABLE
drop table if exists company_people_matcher_tmp;
create table company_people_matcher_tmp as 
    select distinct linkedin_url as company_url, 
    unnest(extract_related_info(employee_details_array,1)) people_url 
    from linkedin_company_base where employee_details_array != '{}' ;

--company linkedin url from people table added to above table.. 
drop table if exists company_people_matcher_tmp2;
create table company_people_matcher_tmp2 as
    select distinct a.company_url, a.people_url,
    unnest(extract_related_info(company_linkedin_url_array,1)) company_url_people_table
    from company_people_matcher_tmp a join linkedin_people_base b
    on a.people_url = b.linkedin_url 
    where company_linkedin_url_array != '{}';

--comapny urls from company table and people table grouped and count taken
--when for a company url from company table has only 1 url from people table
--or a url from people table is present for a url from company table >40% of time, the url from
--people table is selected
drop table if exists company_people_matcher_tmp3;
create table company_people_matcher_tmp3 as
    SELECT company_url, company_url_people_table, group_rows, total_rows , group_rows/total_rows as probability
      FROM (SELECT company_url, company_url_people_table, ROW_NUMBER() OVER (PARTITION BY company_url ORDER BY group_rows DESC) AS rn,
                 group_rows, sum(group_rows) OVER (PARTITION BY company_url) as total_rows
              FROM (  SELECT company_url, company_url_people_table, COUNT(*) AS group_rows 
                        FROM company_people_matcher_tmp2
                    GROUP BY 1, 2) url_freq) ranked_url_req
     WHERE rn = 1 or group_rows/total_rows > 0.4;

delete from company_people_matcher_tmp3 where reverse(split_part(reverse(company_url),'/',1)) !~ '^[0-9]+$';

#only case where this has multiple rows is when no of rows for a company_url is 2 and both people_url are different.
#will delete those cases/keep them? for now delete them?
drop table if exists company_people_matcher_tmp4;
create table company_people_matcher_tmp4 as select * from company_people_matcher_tmp3
    where probability>0.5;

#insert to ref table
insert into company_urls_mapper select linkedin_url as base_url, linkedin_url as alias_url from 
    linkedin_company_base where reverse(split_part(reverse(linkedin_url),'/',1)) !~ '^[0-9]+$'
    on conflict do nothing;

insert into company_urls_mapper select company_url_people_table as base_url,company_url as alias_url
    from company_people_matcher_tmp4 on conflict do nothing;
insert into company_urls_mapper select company_url_people_table as base_url,company_url_people_table as alias_url
    from company_people_matcher_tmp4 on conflict do nothing;

#people company mapper
create table people_company_mapper as
    select distinct * from (
    (select linkedin_url as people_url, unnest(company_linkedin_url_array) as company_url
    from linkedin_people_base where company_linkedin_url_array != '{}')
   union
    (select unnest(employee_details_array) as people_url,
    linkedin_url as company_url from linkedin_company_base where
    employee_details_array != '{}')
    )a;

create index on people_company_mapper(people_url);
create index on people_company_mapper(company_url);
create unique index on people_company_mapper(company_url,people_url);

#need to add urls obtained from people table also into the company_urls_mapper table
insert into company_urls_mapper select company_url as base_url,company_url as alias_url
    from people_company_mapper on conflict do nothing;


#querying example
select distinct * from 
    (select a.company_name,b.name,b.sub_text from linkedin_company_base a join 
        company_urls_mapper c on a.linkedin_url = c.alias_url join 
        people_company_mapper d on c.base_url=d.company_url join 
        linkedin_people_base b on d.people_url = b.linkedin_url 
        limit 1000)a;

#data for dhanesh
create table data_for_email_extraction as 
    select distinct b.linkedin_url,b.name,a.website,a.linkedin_url as company_linkedin_url 
    from linkedin_company_base a join 
        company_urls_mapper c on a.linkedin_url = c.alias_url join 
        people_company_mapper d on c.base_url=d.company_url join 
        linkedin_people_base b on d.people_url = b.linkedin_url
        where website is not null and website != 'NULL' and b.name is not null 
        and b.name != 'NULL' and a.industry in
('Computer-Software','Computer Games',
'Computer & Network Security',
'Computer Networking','Computer Software',
'Information Technology and Services',
'İnternet','Consumer Electronics',
'Computer Hardware','Industrial Automation',
'E-Learning','Internet');



######creating people url mapper
drop table if exists linkedin_people_distinct_tmp1;
drop table if exists linkedin_people_distinct_tmp2;
create table linkedin_people_distinct_tmp1 as 
    select distinct trim(name) name,trim(company_name) company_name,trim(sub_text) sub_text,trim(location) location,trim(industry) industry 
    from linkedin_people_base;
create table linkedin_people_distinct_tmp2 as select a.name,a.company_name,a.sub_text,a.location,a.industry,
    array_agg(b.linkedin_url) linkedin_urls_array
    from linkedin_people_distinct_tmp1 a join linkedin_people_base b 
    on a.name=b.name and a.company_name=b.company_name and a.sub_text=b.sub_text and a.location=b.location and a.industry=b.industry
    group by 1,2,3,4,5;
alter table linkedin_people_distinct_tmp2 add column linkedin_base_url text;
update linkedin_people_distinct_tmp2 set linkedin_base_url = linkedin_urls_array[1];
drop table if exists linkedin_people_url_mapper;
create table linkedin_people_url_mapper as select distinct linkedin_base_url as base_url,unnest(linkedin_urls_array) as alias_url 
    from linkedin_people_distinct_tmp2;
create table linkedin_people_url_mapper_extra as select a.base_url,b.base_url as alias_url 
    from linkedin_people_url_mapper a join linkedin_people_url_mapper b on a.alias_url=b.alias_url and a.base_url!= b.base_url and a.base_url<b.base_url 
    where linkedin_url_similarity(a.base_url,b.base_url) = 1 ;
