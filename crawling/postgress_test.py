__author__ = 'joswin'
#####postgress details
# refer https://help.ubuntu.com/community/PostgreSQL
#created user called postgress using command : 'sudo -u postgres psql postgres'
#set password for this user as 'postgres

'''
alter table linkedin_company_base rename to linkedin_company_base_2016_06_10_2;
alter table linkedin_people_base rename to linkedin_people_base_2016_06_10_2;

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
CREATE TABLE linkedin_company_urls_to_crawl (
    url text UNIQUE
);
CREATE TABLE linkedin_people_urls_to_crawl (
    url text UNIQUE
);
CREATE TABLE linkedin_company_finished_urls (
    url text UNIQUE
);
CREATE TABLE linkedin_people_finished_urls (
    url text UNIQUE
);
CREATE TABLE linkedin_people_urls_to_crawl_priority (
    url text UNIQUE
);
CREATE TABLE linkedin_company_urls_to_crawl_priority (
    url text UNIQUE
);

'''
import pdb
import re
from crawler_generic import LinkedinCrawlerThread
from postgres_connect import PostgresConnect

cc = LinkedinCrawlerThread()
crawled_loc = 'crawled_res/'
crawled_files_company = cc.get_files_in_dir(crawled_loc,match_regex='^company.+\.txt$')
crawled_files_company.sort()
# lcc.con.get_cursor()
table_fields = ['Linkedin URL','Company Name','Company Size','Industry','Type','Headquarters',
                                 'Description Text','Founded','Specialties','Website'
                ,'Employee Details','Also Viewed Companies']
table_field_names = ['linkedin_url','company_name','company_size','industry','company_type','headquarters',
                                      'description','founded','specialties','website','employee_details','also_viewed_companies']
base_table = 'linkedin_company_base_2015_06_25_full'
con = PostgresConnect()
con.get_cursor()

def save_to_table(res):
    if res.get('Employee Details',[]):
        employee_urls = [re.split('\?trk',com_dic.get('linkedin_url',''))[0] + '{}' +
                         com_dic.get('Name','') + '{}' + com_dic.get('Designation','')
                         for com_dic in res['Employee Details']]
        res['Employee Details'] = '|'.join(employee_urls)
    else:
        res['Employee Details'] = ''
    if res.get('Also Viewed Companies',[]):
        also_viewed_urls = [re.split('\?trk',com_dic.get('company_linkedin_url',''))[0] + '{}' +
                            com_dic.get('Company Name','')
                            for com_dic in res['Also Viewed Companies']]
        res['Also Viewed Companies'] = '|'.join(also_viewed_urls)
    else:
        res['Also Viewed Companies'] = ''
    res_fields = []
    for field in table_fields:
        field_val = res.get(field,'NULL')
        res_fields.append(field_val)
    query = '''INSERT INTO {} ({}) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ) '''.format(base_table,','.join(table_field_names))
    con.cursor.execute(query,res_fields)
    con.commit()

import logging
logging.basicConfig(filename='txt_files_to_table.log', level=logging.INFO,format='%(asctime)s %(message)s')
for f_name in crawled_files_company:
    print f_name
    with open(f_name,'r') as f_in:
        for line in f_in:
            # pdb.set_trace()
            try:
                line_dic = eval(line)
                save_to_table(line_dic)
            except:
                logging.info('error happened for data : {}'.format(line_dic))


#####################################################
#############for people table
import pdb
import re
from crawler_generic import LinkedinCrawlerThread
from postgres_connect import PostgresConnect

cc = LinkedinCrawlerThread()
crawled_loc = 'crawled_res/'
crawled_files_people = cc.get_files_in_dir(crawled_loc,match_regex='^people.+\.txt$')
crawled_files_people.sort()
con = PostgresConnect()
con.get_cursor()
table_fields = ['Linkedin URL','Name','Position','Location','Company','CompanyLinkedinPage',
                                 'PreviousCompanies','Education','Industry','Summary','Skills','Experience'
                                 ,'Related People','Same Name People']
table_field_names = ['linkedin_url','name','sub_text','location','company_name','company_linkedin_url',
                                      'previous_companies','education','industry','summary','skills','experience',
                                      'related_people','same_name_people']
base_table = 'linkedin_people_base_2015_06_06_full'

def save_to_table(res):
    if res.get('Related People',[]):
        related_people_urls = [re.split('\?trk',com_dic.get('Linkedin Page',''))[0] + '{}' +
                               com_dic.get('Name','') + '{}' + com_dic.get('Position')
                               for com_dic in res['Related People']]
        res['Related People'] = '|'.join(related_people_urls)
    else:
        res['Related People'] = ''
    if res.get('Same Name People',[]):
        same_name_urls = [re.split('\?trk',com_dic.get('Linkedin Page',''))[0] + '{}' +
                          com_dic.get('Name','') + '{}' + com_dic.get('Position','')
                          for com_dic in res['Same Name People']]
        res['Same Name People'] = '|'.join(same_name_urls)
    else:
        res['Same Name People'] = ''
    if res.get('Experience',[]):
        company_urls = [re.split('\?trk',com_dic.get('Company Linkedin',''))[0] + '{}'+
                        com_dic.get('Position','') + '{}' + com_dic.get('Company','') + '{}' +
                        com_dic.get('Date Range','') + '{}' + com_dic.get('Location','') + '{}' +
                        com_dic.get('Description','')
                        for com_dic in res['Experience']]
        res['Experience'] = '|'.join(company_urls)
    else:
        res['Experience'] = ''
    res['CompanyLinkedinPage'] = re.split('',res.get('CompanyLinkedinPage',''))[0]
    res_fields = []
    for field in table_fields:
        field_val = res.get(field,'NULL')
        res_fields.append(field_val)
    query = 'INSERT INTO {} ({}) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ) '.format(base_table,','.join(table_field_names))
    con.cursor.execute(query,res_fields)
    con.commit()

import logging
logging.basicConfig(filename='txt_files_to_table_people.log', level=logging.INFO,format='%(asctime)s %(message)s')
for f_name in crawled_files_people:
    print f_name
    with open(f_name,'r') as f_in:
        for line in f_in:
            # pdb.set_trace()
            try:
                line_dic = eval(line)
                save_to_table(line_dic)
            except:
                logging.info('error happened for data : {}'.format(line_dic))

# lpc.con.get_cursor()

#getting current company linkedin
'''
alter table linkedin_people_base_date add column current_company_linkedin_url text;
Update linkedin_people_base_date set current_company_linkedin_url = REPLACE((string_to_array(company_linkedin_url,','))[1],'?trk=ppro_cprof','');
create index on linkedin_people_base_date (current_company_linkedin_url);
'''

'''
#creating table from dhanesh 
create table data_for_dhanesh_2016_06_06_1
as 
select distinct
b.people_url as linkedin_url,a.name,c.website,b.company_url as company_linkedin_url
from
linkedin_people_base_2016_06_06 a 
join
company_people_matcher b on split_part(a.linkedin_url,'?trk',1) = b.people_url
join
linkedin_company_base_2016_06_06 c on b.company_url = split_part(c.linkedin_url,'?trk',1)
where
c.website is not null and c.website != 'NULL' and
c.industry in 
('Computer-Software','Computer Games',
'Computer & Network Security',
'Computer Networking','Computer Software',
'Information Technology and Services',
'İnternet','Consumer Electronics',
'Computer Hardware','Industrial Automation',
'E-Learning','Internet')
and employee_details like '%linkedin%'
'''


#######cleaning names
import name_tools
from postgres_connect import PostgresConnect
con = PostgresConnect()
con.get_cursor()
query = 'select * from data_for_email_extraction2'
con.execute(query)
row = con.cursor.fetchone()
while row:
    name = row[1]
    name_cleaned = name_tools.split(name)
    f_part = name_cleaned[1].split()
    if len(f_part) == 1:
        f_name,m_name = f_part[0],''
    elif len(f_part) > 1:
        f_name,m_name = f_part[0],' '.join(f_part[1:])
    else:
        f_name,m_name = '',''
    name_list = [name_cleaned[0],f_name,m_name,name_cleaned[2],name_cleaned[3]]
    row_nu = row[7]
    query = 'update data_for_email_extraction2 set name_cleaned = %s where rnum = {}'.format(row_nu)
    con.cursor.execute(query,(name_list,))

con.commit()

'''using plpython to clean names

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


create table data_for_email_extraction2 as select distinct name,
name_cleaned,name_cleaned[2] as first_name,name_cleaned[3] as middle_name,name_cleaned[4] last_name,
replace(substring(website  from '.*://([^/]*)'),'www.','') as domain,sub_text
from data_for_email_extraction1 where website not in ('http://','http://-','http://.','http://...','http://1','')
and name_cleaned[2] is not null and name_cleaned[4] is not null and name_cleaned[2] not in('','.','..')
and name_cleaned[4] not in ('','.','..') and website is not null;
alter table data_for_email_extraction2 add column linkedin_url text;
update data_for_email_extraction2 as a set linkedin_url=b.linkedin_url from data_for_email_extraction1 as b
where a.name=b.name and a.domain=replace(substring(b.website  from '.*://([^/]*)'),'www.','') and a.sub_text=b.sub_text;
delete from data_for_email_extraction2 where domain = '' or (first_name ='' and last_name ='');

'''