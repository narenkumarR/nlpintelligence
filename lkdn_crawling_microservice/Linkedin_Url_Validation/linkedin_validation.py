import re

import proc as proc
import psycopg2
from optparse import OptionParser
from fuzzywuzzy import fuzz

class linkedinUrlValidation (object):
    def __init__(self):
        # self.db_insert = ''
        # type: () -> object
        try:
            self.conn = psycopg2.connect(
                "dbname='crawler_service_test' user='postgres' host='192.168.3.56' password='postgres'")
            self.conn.autocommit = True
            self.db_insert = self.conn.cursor()
            print("connected successfully")
        except:
            print("Not unable to connect to the database")

        print('process started')

    def validate_linkedin(self,list_name):

        linkedin_url_query_extract='select id from crawler.list_table where list_name ={}'.format(list_name)
        self.db_insert.execute(linkedin_url_query_extract)
        list_name_info=self.db_insert.fetchall()
        process_list_name=''
        for list_name_complete_info in list_name_info:
             process_list_name= "'"+list_name_complete_info[0]+"'"


        linkedin_validate_obj.get_linkedin_url_from_table(process_list_name)


    def get_linkedin_url_from_table(self,list_name):
        linkedin_url='select a.list_input,list_input_additional,url from crawler.list_items a left join crawler.list_items_urls b \
        on a.list_id=b.list_id and a.id=b.list_items_id where a.list_id = {} and b.url is not null'.format(list_name)
        # print(linkedin_url)
        self.db_insert.execute(linkedin_url)
        company_name_and_linkedin_url = self.db_insert.fetchall()
        for cmp_name_linkedin_urls in company_name_and_linkedin_url:
            list_input=cmp_name_linkedin_urls[0]
            company_name=cmp_name_linkedin_urls[1]
            linkedin_url=cmp_name_linkedin_urls[2]
            linkedin_url_company_name_extract=str(linkedin_url)
            company_name_from_linkedin_url=linkedin_url[33:len(linkedin_url_company_name_extract)]
            validate_tup=(company_name,company_name_from_linkedin_url,linkedin_url)
            # print(linkedin_url)
            # print(company_name_from_linkedin_url)
            # print(company_name)
            # exit()
            # print(linkedin_url_company_name_extract)
            # print(len(linkedin_url_company_name_extract)-21)
            linkedin_validate_obj.check_linkedin_url_with_company_name(company_name,company_name_from_linkedin_url,linkedin_url,list_input)



    def check_linkedin_url_with_company_name(self,company_name,company_name_from_linkedin_url,linkedin_url,list_input):
        # print(company_name,' :',company_name_from_linkedin_url,' :',linkedin_url)
        fuzzy_score=fuzz.token_set_ratio(company_name,company_name_from_linkedin_url)
        if(fuzzy_score >=10):
             print(company_name, ' :', company_name_from_linkedin_url, ' :', linkedin_url,list_input)
             print(fuzzy_score)



if __name__ == "__main__":
    optparser = OptionParser()
    optparser.add_option('-n', '--lname',
                         dest='list_name',
                         help='list name',
                         default=None)

    (options, args) = optparser.parse_args()
    list_name = options.list_name


    linkedin_validate_obj = linkedinUrlValidation()
    linkedin_validate_obj.validate_linkedin(list_name)




