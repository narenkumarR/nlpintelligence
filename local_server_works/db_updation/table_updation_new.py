__author__ = 'joswin'

import psycopg2
from optparse import OptionParser
from constants import to_database,to_host,to_password,to_user,to_port

class LinkedinDumpUtil(object):
    '''
    '''
    def __init__(self,con=False):
        '''
        :return:
        '''
        if not con:
            self.con = psycopg2.connect(database=to_database, user=to_user,password=to_password,host=to_host)
            # self.cursor = self.con.cursor()
        else:
            self.con = con
            # self.cursor = self.con.cursor()

    def process_company_dump(self,table_name,con=None,array_present=False,timestamp_col = 'timestamp'):
        '''
        :param table_name:
        :param con:
        :param array_present : if the table has arrays
        :param timestamp_col : older tables have timestamp, newer tables have created_on. give the name here
        :return:
        '''
        print('started company dumping')
        if con:
            self.con = con
        self.cursor = self.con.cursor()
        query = "delete from "+table_name+" where linkedin_url like '%,%' or linkedin_url like '%|%' or linkedin_url like '%{}%' or linkedin_url not like '%linkedin%'"
        self.cursor.execute(query)
        #all urls starting with http needs to be converted to https
        query = "update {} set linkedin_url = concat('https:',split_part(linkedin_url,'http:',2)) where linkedin_url like 'http:%'".format(table_name)
        self.cursor.execute(query)
        self.con.commit()
        query = "update {} set linkedin_url = split_part(linkedin_url,'?trk',1)".format(table_name)
        self.cursor.execute(query)
        if not array_present:
            query = 'alter table {} add column employee_details_array text[], add column also_viewed_companies_array text[]'.format(table_name)
            self.cursor.execute(query)
            self.con.commit()
            query = "update {} set employee_details_array = clean_linkedin_url_array(regexp_split_to_array(replace(employee_details,'NULL',''),'[|]')) "\
                        " where employee_details like '%linkedin%'".format(table_name)
            self.cursor.execute(query)
            query = "update {} set also_viewed_companies_array = clean_linkedin_url_array(regexp_split_to_array(replace(also_viewed_companies,'NULL',''),'[|]')) "\
                        " where also_viewed_companies like '%linkedin%'".format(table_name)
            self.cursor.execute(query)
            self.con.commit()
        insert_query = "insert into linkedin_company_base "\
                "(linkedin_url,company_name,company_size,industry,company_type,headquarters,description,founded,"\
                "specialties,website,timestamp,employee_details_array,also_viewed_companies_array) " \
                       "select linkedin_url,company_name,company_size,industry,company_type,headquarters,description,founded,\
                specialties,website,{},employee_details_array,also_viewed_companies_array from {}".format(timestamp_col,table_name)
        self.cursor.execute(insert_query)
        self.con.commit()
        mapper_query = "insert into people_company_mapper "\
                " select split_part(unnest(extract_related_info(employee_details_array,1)),'?trk',1) as people_url,linkedin_url as company_url "\
                "from "+table_name+" where employee_details_array != '{}' on conflict do nothing"
        self.cursor.execute(mapper_query)
        self.con.commit()
        #bw_map_query = "insert into builtwith_linkedin_mapper select linkedin_url url_linkedin_table,\"Domain\" as domain_builtwith_table from "\
        #        " {} a join builtwith_companies_meta_data b on "\
        #   " (split_part(linkedin_url,'www.',2) = \"LinkedIn\" and linkedin_url!='' and "\
        #   " linkedin_url!='NULL' and \"LinkedIn\"!='' and \"LinkedIn\" is not null) "\
        #   " or (split_part(website,'www.',2)=\"Domain\" and \"Domain\"!='' and website != '' and "\
        #   " website!='NULL' and \"Domain\" is not null and website is not null) on conflict do nothing".format(table_name)
        #self.cursor.execute(bw_map_query)
        # updating domains in the domain table (need to remove wrong domains like facebook etc from this)
        # self.cursor.execute('drop table if exists linkedin_company_domains_tmp_table')
        # self.con.commit()
        # need to update this to include the new columns in domains table
        # query = "create table linkedin_company_domains_tmp_table "
        # query = "insert into linkedin_company_domains (linkedin_url,domain) "\
        #         " select linkedin_url,replace(substring(website  from '.*://([^/]*)'),'www.','') as domain "\
        #         " from {} "\
        #         "on conflict do nothing".format(table_name)
        #self.cursor.execute(query)
        # drop_query = "drop table if exists {}".format(table_name)
        # self.cursor.execute(drop_query)

        # updating linkedin_domains table
        self.cursor.execute('drop table if exists {}_domain_tmp'.format(table_name))
        self.con.commit()
        query = "create table {tab_name}_domain_tmp as "\
            " select linkedin_url,replace(substring(website  from '.*://([^/]*)'),'www.','') as domain, "\
            " headquarters from {tab_name}".format(tab_name=table_name)
        self.cursor.execute(query)
        self.con.commit()
        query = "alter table {}_domain_tmp add column linkedin_name text, add column website_cleaned text, "\
            " add column location text, add column region text, add column country text".format(table_name)
        self.cursor.execute(query)
        self.con.commit()
        query = "update {}_domain_tmp set website_cleaned = domain".format(table_name)
        self.cursor.execute(query)
        self.con.commit()
        queries = (
            "update {}_domain_tmp set linkedin_name = split_part(linkedin_url,'linkedin.com/company/',2) "\
            " where  linkedin_url like '%linkedin.com/company/%'".format(table_name),
            "update {}_domain_tmp set linkedin_name = split_part(linkedin_url,'linkedin.com/companies/',2) "\
            " where  linkedin_url like '%linkedin.com/companies/%'".format(table_name),
            "update {}_domain_tmp set domain = '' where domain like '%google.com%' and linkedin_name != 'google'".format(table_name),
            "update {}_domain_tmp set domain = '' where domain like '%facebook.com%' and linkedin_name !=  'facebook'".format(table_name),
            "update {}_domain_tmp set domain = '' where domain like '%linkedin.com%' and linkedin_name != 'linkedin'".format(table_name),
            "update {}_domain_tmp set domain = '' where domain like '%yahoo.com%' and (linkedin_name != 'yahoo' and linkedin_name != '1288') ".format(table_name),
            "update {}_domain_tmp set domain = '' where domain like '%twitter.com%' and linkedin_name != 'twitter'".format(table_name),
            "update {}_domain_tmp set domain = '' where domain like '%myspace.com%' and linkedin_name != 'myspace'".format(table_name),
            "update {}_domain_tmp set domain = '' where domain like '%yelp.com%' and linkedin_name != 'Yelp'".format(table_name),
            "update {}_domain_tmp set domain = '' where domain = 'none'".format(table_name),
            "update {}_domain_tmp set domain = '' where domain = 'N'".format(table_name),
            "update {}_domain_tmp set domain = '' where domain in ( 'n','http','-','TBD','TBA')".format(table_name),
            "update {}_domain_tmp set domain = '' where domain in ('www','http:','.','None','NA','sites.google.com','0','fb.com','x','ow.ly')".format(table_name),
            "update {}_domain_tmp set domain = '' where domain like '%youtube.com%' and linkedin_name != 'youtube'".format(table_name),
            "update {}_domain_tmp set domain = '' where domain in ('na.na','na','http;','goo.gl','1',"\
                "'plus.google.com','com','itunes.apple.com','underconstruction.com')".format(table_name),
            "update {}_domain_tmp set domain = '' where domain ='gov.uk'".format(table_name),
            "update {}_domain_tmp set domain = '' where domain like '%meetup.com%' and linkedin_name != 'meetup'".format(table_name),
            "update {}_domain_tmp set domain = '' where domain like '%wikipedia.org%' and linkedin_name != 'wikipedia'".format(table_name),
            "update {}_domain_tmp set domain = '' where domain like '%angel.co%' and linkedin_name != 'angellist'".format(table_name),
            "update {}_domain_tmp set domain = '' where domain like '%vimeo.com%' and linkedin_name != 'vimeo'".format(table_name),
            "update {}_domain_tmp set domain = '' where domain like '%instagram.com%' and linkedin_name != 'instagram'".format(table_name),
            "update {}_domain_tmp set domain = '' where domain like '%companycheck.co.uk%' and linkedin_name != 'company-check-ltd'".format(table_name),
            "update {}_domain_tmp set domain = '' where domain like '%tinyurl.com%' and linkedin_name != 'tinyurl'".format(table_name)
            )
        for query in queries:
            self.cursor.execute(query)
            self.con.commit()        
        # now update location info
        queries = (
            "update {}_domain_tmp a set country=b.country from location_tabs.countries_ref_table_regex b "\
                " where (headquarters like country_code_like or headquarters ilike country_like) ".format(table_name),
            "update {}_domain_tmp a set region=b.region,country=b.country from location_tabs.regions_ref_table_regex b "\
                " where (headquarters like region_code_like or headquarters ilike region_like) and "\
                " (headquarters like country_code_like or headquarters ilike country_like) ".format(table_name),
            "update {}_domain_tmp a set location = b.location_cleaned,region=b.region,country=b.country "\
                " from location_tabs.locations_ref_table_regex b "\
                " where (headquarters like location_code_like or headquarters ilike location_like) and "\
                " (headquarters like region_code_like or headquarters ilike region_like) and "\
                " (headquarters like country_code_like or headquarters ilike country_like)".format(table_name)
            )
        for query in queries:
            self.cursor.execute(query)
            self.con.commit() 
        query = "insert into linkedin_company_domains "\
                    " (linkedin_url, domain, linkedin_name, "\
                    " website_cleaned, location, region, country, headquarters) "\
                    "  select linkedin_url, domain, linkedin_name, "\
                    " website_cleaned, location, region, country, headquarters "\
                    " from {}_domain_tmp".format(table_name)
        self.cursor.execute(query)
        self.con.commit()  
        self.cursor.close()
        print('finished company dump')

    def process_people_dump(self,table_name,con=None,array_present=False,timestamp_col='timestamp'):
        '''
        :param table_name:
        :param con:
        :param array_present:
        :param timestamp_col : older tables have timestamp, newer tables have created_on. give the name here
        :return:
        '''
        print('started people dumping')
        if con:
            self.con = con
        self.cursor = self.con.cursor()
        query = "delete from "+table_name+" where linkedin_url like '%,%' or linkedin_url like '%|%' or linkedin_url like '%{}%' or linkedin_url not like '%linkedin%'"
        self.cursor.execute(query)
        #all urls starting with http needs to be converted to https
        query = "update {} set linkedin_url = concat('https:',split_part(linkedin_url,'http:',2)) where linkedin_url like 'http:%'".format(table_name)
        self.cursor.execute(query)
        self.con.commit()
        query = "update {} set linkedin_url = split_part(linkedin_url,'?trk',1)".format(table_name)
        self.cursor.execute(query)
        if not array_present:
            query = 'alter table {} add column company_linkedin_url_array text[], add column related_people_array text[],'\
                    'add column same_name_people_array text[], add column experience_array text[]'.format(table_name)
            self.cursor.execute(query)
            self.con.commit()
            query = "update {} set company_linkedin_url_array = clean_linkedin_url_array(regexp_split_to_array(replace(company_linkedin_url,'NULL',''),'[|]')) \
                        where company_linkedin_url like '%linkedin%'".format(table_name)
            self.cursor.execute(query)
            query = "update {} set related_people_array = clean_linkedin_url_array(regexp_split_to_array(replace(related_people,'NULL',''),'[|]')) \
                        where related_people like '%linkedin%'".format(table_name)
            self.cursor.execute(query)
            query = "update {} set same_name_people_array = clean_linkedin_url_array(regexp_split_to_array(replace(same_name_people,'NULL',''),'[|]')) \
                        where same_name_people like '%linkedin%'".format(table_name)
            self.cursor.execute(query)
            query = "update {} set experience_array = clean_linkedin_url_array(regexp_split_to_array(replace(experience,'NULL',''),'[|]')) \
                        where experience is not null and experience != '' ".format(table_name)
            self.cursor.execute(query)
            self.con.commit()
        insert_query = "insert into linkedin_people_base "\
                "(linkedin_url,name,sub_text,location,company_name,previous_companies,education,industry,summary, "\
                "skills,timestamp,company_linkedin_url_array,related_people_array,same_name_people_array,experience_array) " \
                       "select linkedin_url,name,sub_text,location,company_name,previous_companies,education,industry, "\
                "summary,skills,{},company_linkedin_url_array,related_people_array, "\
                "same_name_people_array,experience_array from {}".format(timestamp_col,table_name)
        self.cursor.execute(insert_query)
        mapper_query = "insert into people_company_mapper "\
                       "select linkedin_url as people_url, split_part(unnest(extract_related_info(company_linkedin_url_array,1)),'?trk',1) as company_url "\
                "from "+table_name+" where company_linkedin_url_array != '{}' on conflict do nothing"
        self.cursor.execute(mapper_query)
        self.cursor.execute("insert into people_urls_mapper (base_url,alias_url) select linkedin_url,linkedin_url from "\
                 " {} on conflict do nothing".format(table_name))
        # drop_query = "drop table if exists {}".format(table_name)
        # self.cursor.execute(drop_query)
        self.con.commit()
        self.cursor.close()
        print('finished people dumping')

    def update_company_mapper(self,company_table,con=None):
        ''' This should be called after calling process_company_dump and process_people_dump tables
        :param company_table:
        :return:
        '''
        print('started company mapper dumping')
        if con:
            self.con = con
        self.cursor = self.con.cursor()
        self.cursor.execute('drop table if exists company_people_matcher_tmp')
        self.cursor.execute('drop table if exists company_people_matcher_tmp2')
        self.cursor.execute('drop table if exists company_people_matcher_tmp3')
        self.cursor.execute('drop table if exists company_people_matcher_tmp4')
        self.con.commit()
        # creating url maps from the new company table
        query = '''create table company_people_matcher_tmp as
            select distinct linkedin_url as company_url,
            unnest(extract_related_info(employee_details_array,1)) people_url
            from {} where employee_details_array != '{{}}' '''.format(company_table)
        self.cursor.execute(query)
        self.con.commit()
        # delete all urls already present in the mapper table
        query = "delete from company_people_matcher_tmp a using company_urls_mapper b where a.company_url=b.alias_url"
        self.cursor.execute(query)
        self.con.commit()
        # here the new linkedin_people_base should be used (using the array columns, not raw columns)
        query = '''create table company_people_matcher_tmp2 as
                        select distinct a.company_url, a.people_url,
                        unnest(extract_related_info(company_linkedin_url_array,1)) company_url_people_table
                        from company_people_matcher_tmp a join linkedin_people_base b
                        on a.people_url = b.linkedin_url
                        where company_linkedin_url_array != '{}' '''
        self.cursor.execute(query)
        self.con.commit()
        query = '''create table company_people_matcher_tmp3 as
                    SELECT company_url, company_url_people_table, group_rows, total_rows , group_rows/total_rows as probability
                      FROM (SELECT company_url, company_url_people_table, ROW_NUMBER() OVER (PARTITION BY company_url ORDER BY group_rows DESC) AS rn,
                                 group_rows, sum(group_rows) OVER (PARTITION BY company_url) as total_rows
                              FROM (  SELECT company_url, company_url_people_table, COUNT(*) AS group_rows
                                        FROM company_people_matcher_tmp2
                                    GROUP BY 1, 2) url_freq) ranked_url_req
                     WHERE rn = 1 or group_rows/total_rows > 0.4 '''
        self.cursor.execute(query)
        self.con.commit()
        query = "delete from company_people_matcher_tmp3 where reverse(split_part(reverse(company_url),'/',1)) !~ '^[0-9]+$' "
        self.cursor.execute(query)
        self.con.commit()
        query = " create table company_people_matcher_tmp4 as select * from company_people_matcher_tmp3 where probability>0.5 "
        self.cursor.execute(query)
        # inserting into the mapper all proper urls in the new company table
        query = ''' insert into company_urls_mapper select linkedin_url as base_url, linkedin_url as alias_url from
                    {} where reverse(split_part(reverse(linkedin_url),'/',1)) !~ '^[0-9]+$'
                    on conflict do nothing '''.format(company_table)
        # insert urls from tmp4 table
        self.cursor.execute(query)
        self.con.commit()
        query = ''' insert into company_urls_mapper select company_url_people_table as base_url,company_url as alias_url
                        from company_people_matcher_tmp4 on conflict do nothing '''
        self.cursor.execute(query)
        self.con.commit()
        # insert actual base urls in the tmp4 table
        query = ''' insert into company_urls_mapper select company_url_people_table as base_url,company_url_people_table as alias_url
                        from company_people_matcher_tmp4 on conflict do nothing '''
        self.cursor.execute(query)
        self.con.commit()
        # need to add urls obtained from people table also into the company_urls_mapper table . here also, the people_company_mapper
        # table should have new data. Otherwise this insert will not insert any new info
        query = ''' insert into company_urls_mapper select company_url as base_url,company_url as alias_url
                        from people_company_mapper on conflict do nothing '''
        self.cursor.execute(query)
        # now insert all remaining urls in the company table, which are not present in the mapper table
        query = "insert into company_urls_mapper select linkedin_url as base_url,linkedin_url as alias_url "\
                " from {} a left join company_urls_mapper b on a.linkedin_url = b.alias_url "\
                " where b.alias_url is null on conflict do nothing".format(company_table)
        self.cursor.execute(query)
        self.con.commit()
        self.cursor.execute('drop table if exists company_people_matcher_tmp')
        self.cursor.execute('drop table if exists company_people_matcher_tmp2')
        self.cursor.execute('drop table if exists company_people_matcher_tmp3')
        self.cursor.execute('drop table if exists company_people_matcher_tmp4')
        query =  "delete from company_urls_mapper a using company_urls_mapper b where "\
                " a.base_url = b.alias_url and reverse(split_part(reverse(a.base_url),'/',1)) ~ '^[0-9]+$' and "\
                " reverse(split_part(reverse(b.base_url),'/',1)) !~ '^[0-9]+$' "
        self.cursor.execute(query)
        self.con.commit()
        self.cursor.close()
        print('finished company mapping')

if __name__ == "__main__":
    optparser = OptionParser()
    optparser.add_option('-C', '--companyTable',
                         dest='company_table',
                         help='Name of company table',
                         default=None)
    optparser.add_option('-P', '--peopleTable',
                         dest='people_table',
                         help='Name of people table',
                         default=None)
    optparser.add_option('-a', '--arrayPresent',
                         dest='array_present',
                         help = 'flag set 1 if details are present in array form in the table',
                         default=0,
                         type='float')
    optparser.add_option('-t', '--timestampCol',
                         dest='timestamp_col',
                         help='Name of timestamp column, default timestamp',
                         default='timestamp')

    (options, args) = optparser.parse_args()

    company_table = options.company_table
    people_table = options.people_table
    array_present = options.array_present
    timestamp_col = options.timestamp_col

    dump_util = LinkedinDumpUtil()
    if people_table:
        dump_util.process_people_dump(people_table,array_present=array_present,timestamp_col=timestamp_col)
    if company_table:
        dump_util.process_company_dump(company_table,array_present=array_present,timestamp_col=timestamp_col)
        dump_util.update_company_mapper(company_table)
    dump_util.con.close()


