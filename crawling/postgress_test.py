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
from crawler_generic import LinkedinCrawlerThread
from crawler import LinkedinCompanyCrawlerThread,LinkedinProfileCrawlerThread
cc = LinkedinCrawlerThread()


lcc = LinkedinCompanyCrawlerThread(use_db=True,proxy=False)
crawled_loc = 'crawled_res/'
crawled_files_company = cc.get_files_in_dir(crawled_loc,match_regex='^company.+\.txt$')
crawled_files_company.sort()
# lcc.con.get_cursor()
for f_name in crawled_files_company:
    print f_name
    with open(f_name,'r') as f_in:
        for line in f_in:
            # pdb.set_trace()
            line_dic = eval(line)
            lcc.save_to_table(line_dic)

lpc = LinkedinProfileCrawlerThread(use_db=True,proxy=False)
crawled_loc = 'crawled_res/'
crawled_files_people = cc.get_files_in_dir(crawled_loc,match_regex='^people.+\.txt$')
crawled_files_people.sort()
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