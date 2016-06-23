__author__ = 'joswin'

import psycopg2
from optparse import OptionParser

class LinkedinDumpUtil(object):
    '''
    '''
    def __init__(self,con=False):
        '''
        :return:
        '''
        if not con:
            self.con = psycopg2.connect(database='linkedin_data', user='postgres',password='postgres',host='localhost')
            # self.cursor = self.con.cursor()
        else:
            self.con = con
            # self.cursor = self.con.cursor()

    def process_company_dump(self,table_name,con=None,array_present=False):
        '''
        :param table_name:
        :param con:
        :param array_present : if the table has arrays
        :return:
        '''
        if con:
            self.con = con
        self.cursor = self.con.cursor()
        if not array_present:
            query = 'alter table {} add column employee_details_array text[], add column also_viewed_companies_array text[]'.format(table_name)
            self.cursor.execute(query)
            self.con.commit()
            query = "update {} set employee_details_array = clean_linkedin_url_array(regexp_split_to_array(replace(employee_details,'NULL',''),'[,|]')) "\
                        " where employee_details like '%linkedin%'".format(table_name)
            self.cursor.execute(query)
            query = "update {} set also_viewed_companies_array = clean_linkedin_url_array(regexp_split_to_array(replace(also_viewed_companies,'NULL',''),'[,|]')) "\
                        " where also_viewed_companies like '%linkedin%'".format(table_name)
            self.cursor.execute(query)
            self.con.commit()
        insert_query = "insert into linkedin_company_base "\
                "(linkedin_url,company_name,company_size,industry,company_type,headquarters,description,founded,"\
                "specialties,website,timestamp,employee_details_array,also_viewed_companies_array) " \
                       "select linkedin_url,company_name,company_size,industry,company_type,headquarters,description,founded,\
                specialties,website,timestamp,employee_details_array,also_viewed_companies_array from {}".format(table_name)
        self.cursor.execute(insert_query)
        mapper_query = "insert into people_company_mapper "\
                " select unnest(employee_details_array) as people_url,linkedin_url as company_url "\
                " from "+table_name+" where employee_details_array != '{}' on conflict do nothing"
        self.cursor.execute(mapper_query)
        # drop_query = "drop table if exists {}".format(table_name)
        # self.cursor.execute(drop_query)
        self.con.commit()
        self.cursor.close()

    def process_people_dump(self,table_name,con=None,array_present=False):
        '''
        :param table_name:
        :param con:
        :param array_present:
        :return:
        '''
        if con:
            self.con = con
        self.cursor = self.con.cursor()
        if not array_present:
            query = 'alter table {} add column company_linkedin_url_array text[], add column related_people_array text[], '\
                    'add column same_name_people_array text[]'.format(table_name)
            self.cursor.execute(query)
            self.con.commit()
            query = "update {} set company_linkedin_url_array = clean_linkedin_url_array(regexp_split_to_array(replace(company_linkedin_url,'NULL',''),'[,|]')) \
                        where company_linkedin_url like '%linkedin%'".format(table_name)
            self.cursor.execute(query)
            query = "update {} set related_people_array = clean_linkedin_url_array(regexp_split_to_array(replace(related_people,'NULL',''),'[,|]')) \
                        where related_people like '%linkedin%'".format(table_name)
            self.cursor.execute(query)
            query = "update {} set same_name_people_array = clean_linkedin_url_array(regexp_split_to_array(replace(same_name_people,'NULL',''),'[,|]')) \
                        where same_name_people like '%linkedin%'".format(table_name)
            self.cursor.execute(query)
            self.con.commit()
        insert_query = "insert into linkedin_people_base "\
                "(linkedin_url,name,sub_text,location,company_name,previous_companies,education,industry,summary, "\
                "skills,experience,timestamp,company_linkedin_url_array,related_people_array,same_name_people_array) " \
                       "select linkedin_url,name,sub_text,location,company_name,previous_companies,education,industry, "\
                "summary,skills,experience,timestamp,company_linkedin_url_array,related_people_array, "\
                "same_name_people_array from {}".format(table_name)
        self.cursor.execute(insert_query)
        mapper_query = "insert into people_company_mapper "\
                       "select linkedin_url as people_url, unnest(company_linkedin_url_array) as company_url "\
                "from "+table_name+" where company_linkedin_url_array != '{}' on conflict do nothing"
        self.cursor.execute(mapper_query)
        # drop_query = "drop table if exists {}".format(table_name)
        # self.cursor.execute(drop_query)
        self.con.commit()
        self.cursor.close()

    def update_company_mapper(self,company_table,con=None):
        ''' This should be called after calling process_company_dump and process_people_dump functions
        :param company_table:
        :return:
        '''
        if con:
            self.con = con
        self.cursor = self.con.cursor()
        self.cursor.execute('drop table if exists company_people_matcher_tmp')
        self.cursor.execute('drop table if exists company_people_matcher_tmp2')
        self.cursor.execute('drop table if exists company_people_matcher_tmp3')
        self.cursor.execute('drop table if exists company_people_matcher_tmp4')
        self.con.commit()
        # creating url maps from the new company table
        query = "create table company_people_matcher_tmp as "\
            " select distinct linkedin_url as company_url,"\
            " unnest(employee_details_array) people_url "\
            " from {} where employee_details_array != '{{}}' ".format(company_table)
        self.cursor.execute(query)
        # here the new linkedin_people_base should be used.
        query = "create table company_people_matcher_tmp2 as "\
                        " select distinct a.company_url, a.people_url, "\
                        " unnest(company_linkedin_url_array) company_url_people_table "\
                        " from company_people_matcher_tmp a join linkedin_people_base b "\
                        " on a.people_url = b.linkedin_url "\
                        " where company_linkedin_url_array != '{}' "
        self.cursor.execute(query)
        query = '''create table company_people_matcher_tmp3 as
                    SELECT company_url, company_url_people_table, group_rows, total_rows , group_rows/total_rows as probability
                      FROM (SELECT company_url, company_url_people_table, ROW_NUMBER() OVER (PARTITION BY company_url ORDER BY group_rows DESC) AS rn,
                                 group_rows, sum(group_rows) OVER (PARTITION BY company_url) as total_rows
                              FROM (  SELECT company_url, company_url_people_table, COUNT(*) AS group_rows
                                        FROM company_people_matcher_tmp2
                                    GROUP BY 1, 2) url_freq) ranked_url_req
                     WHERE rn = 1 or group_rows/total_rows > 0.4 '''
        self.cursor.execute(query)
        query = "delete from company_people_matcher_tmp3 where reverse(split_part(reverse(company_url),'/',1)) !~ '^[0-9]+$' "
        self.cursor.execute(query)
        query = " create table company_people_matcher_tmp4 as select * from company_people_matcher_tmp3 where probability>0.5 "
        self.cursor.execute(query)
        # inserting into the mapper all proper urls in the new company table
        query = ''' insert into company_urls_mapper select linkedin_url as base_url, linkedin_url as alias_url from
                    {} where reverse(split_part(reverse(linkedin_url),'/',1)) !~ '^[0-9]+$'
                    on conflict do nothing '''.format(company_table)
        # insert urls from tmp4 table
        self.cursor.execute(query)
        query = ''' insert into company_urls_mapper select company_url_people_table as base_url,company_url as alias_url
                        from company_people_matcher_tmp4 on conflict do nothing '''
        self.cursor.execute(query)
        # insert actual base urls in the tmp4 table
        query = ''' insert into company_urls_mapper select company_url_people_table as base_url,company_url_people_table as alias_url
                        from company_people_matcher_tmp4 on conflict do nothing '''
        self.cursor.execute(query)
        # need to add urls obtained from people table also into the company_urls_mapper table . here also, the people_company_mapper
        # table should have new data. Otherwise this insert will not insert any new info
        query = ''' insert into company_urls_mapper select company_url as base_url,company_url as alias_url
                        from people_company_mapper on conflict do nothing '''
        self.cursor.execute(query)
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

    (options, args) = optparser.parse_args()

    company_table = options.company_table
    people_table = options.people_table
    array_present = options.array_present

    dump_util = LinkedinDumpUtil()
    if people_table:
        dump_util.process_people_dump(people_table,array_present=array_present)
    if company_table:
        dump_util.process_company_dump(company_table,array_present=array_present)
        dump_util.update_company_mapper(company_table)
    dump_util.con.close()
