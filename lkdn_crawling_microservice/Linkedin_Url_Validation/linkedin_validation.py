#!/usr/bin/python

from optparse import OptionParser
from fuzzywuzzy import fuzz
from postgres_connect import PostgresConnect
import logging
import tldextract
import psycopg2


class linkedinUrlValidation (object):


    def __init__(self):
        # self.db_insert = ''
        # type: () -> object
        try:
            # self.conn = psycopg2.connect(
            #     "dbname='crawler_service_test' user='postgres' host='192.168.3.56' password='postgres'")
            # self.conn.autocommit = True
            # self.db_insert = self.conn.cursor()
            # print("connected successfully")


            self.conn = PostgresConnect()
            self.conn.autocommit = True
            self.db_insert = self.conn.get_cursor()

            print("connected successfully")
        except:
            print("Not unable to connect to the database")

        logging.exception('linkedin validation process started')
        print ('linkedin validation process started')



    def validate_linkedin(self,list_name):

        try:

            linkedin_url_query_extract='select id from crawler.list_table where list_name ={}'.format(list_name)
            self.db_insert.execute(linkedin_url_query_extract)
            list_name_info=self.db_insert.fetchall()
            logging.exception('Fetching list_id from crawler.list_table executed')
            process_list_id= list_name_info[0][0]
            logging.exception('List ID Feteched from table :{}'.format(process_list_id))

            self.get_details_to_validate(process_list_id)


        except Exception, e:
            logging.exception('Issue in executing list_id fetch Query:{}'.format(linkedin_url_query_extract),str(e))
            print(str(e))






    def get_details_to_validate (self,list_id):

            try:
                get_company_website_from_linkedin_and_input_data='select distinct on (company_name) company_name as linkedin_company_name,website as linkedin_company_website,' \
                                                                   'list_input_additional as input_company_name, list_input as input_company_website,linkedin_url from crawler.linkedin_company_base a ' \
                                                                   'join crawler.linkedin_company_redirect_url b on (a.linkedin_url = b.redirect_url or a.linkedin_url = b.url) join crawler.list_items_urls c on ' \
                                                                   '(b.redirect_url=c.url or b.url=c.url)' \
                                                                   ' and a.list_id=c.list_id join crawler.list_items d on c.list_id=d.list_id and c.list_items_id=d.id where a.list_id = %s'


                self.db_insert.execute(get_company_website_from_linkedin_and_input_data,(list_id,))

                get_complete_info_for_validation = self.db_insert.fetchall()

                logging.exception('Fetching distinct company names and website from linkedin_company_base table and input company name and website from list_item is executed')

                for complete_info_for_validation in get_complete_info_for_validation:
                    linkedin_company_name=  complete_info_for_validation[0]
                    linkedin_company_website=  complete_info_for_validation[1]
                    input_company_name = complete_info_for_validation[2]
                    input_company_website =  complete_info_for_validation[3]
                    company_linkedin_url = complete_info_for_validation[4]
                    logging.exception('Retrieved input company name & website and linkedin company & website for validation process')

                    self.check_linkedin_url_with_company_name(linkedin_company_name, linkedin_company_website, input_company_name,input_company_website,list_id,company_linkedin_url)


            except Exception, e:
                logging.exception('Issue in getting company name & website and linkedin company & website for validation process, check "get_details_to_validate" method',str(e))
                print(str(e))


    def check_linkedin_url_with_company_name(self,linkedin_company_name, linkedin_company_website, input_company_name,input_company_website,list_id,company_linkedin_url):

        try :
            linkedin_company_website_str=str(linkedin_company_website)
            linkedin_company_website_str_replace=linkedin_company_website_str.replace("'","")


            linkedin_company_website_domain=tldextract.extract(linkedin_company_website_str_replace).domain


            logging.exception('input company name :{} vs linkedin company name:{}, validation starts'.format(input_company_name,linkedin_company_name))
            fuzzy_score_company_names_compare=fuzz.token_set_ratio(input_company_name,linkedin_company_name)
            logging.exception('input company name vs linkedin company name validation completed with score ={}'.format(fuzzy_score_company_names_compare))



            logging.exception('input company name :{} vs linkedin company website domain name:{}, validation starts'.format(input_company_name,
                                                                                                             linkedin_company_website_domain))
            fuzzy_score_company_website_name_compare = fuzz.token_set_ratio(input_company_name, linkedin_company_website_domain)
            logging.exception('input company name vs linkedin company website domain name validation completed with score ={}'.format(
            fuzzy_score_company_website_name_compare))


            if(fuzzy_score_company_names_compare >=85) or (fuzzy_score_company_website_name_compare >= 70):
                linkedin_company_name_validate_update=str(linkedin_company_name)
                logging.exception('Validation passed linkedin company name from linkedin_company_base table is {}'.format(linkedin_company_name_validate_update))



                try:
                    validation_update = "update temp_linkedin_company_base set isvalid = 1  where list_id = %s and linkedin_url=%s"
                    logging.exception(
                        'validated updated in linkedin_company_base table with flag as 1 in isValid column')

                    self.db_insert.execute(validation_update, (list_id, company_linkedin_url,))

                except Exception, e:
                    logging.exception('Error in executing validation_update_table method ',str(e))
                    print(str(e))




            else:
                try:

                    # print(input_company_name, ' :', linkedin_company_name, ':', input_company_website, ' :',
                    #       linkedin_company_website_domain, ':', company_linkedin_url)
                    # print(fuzzy_score_company_website_name_compare, ':', fuzzy_score_company_names_compare)

                    inValidation_update = "update temp_linkedin_company_base set isvalid = 0  where list_id = %s and linkedin_url=%s and (isvalid!=1 or isvalid is null)"
                    logging.exception(
                        'inValid records updated in linkedin_company_base table with flag as 0 in isValid column')
                    self.db_insert.execute(inValidation_update, (list_id,company_linkedin_url,))

                except Exception, e:
                    logging.exception('Error in executing inValidation_update_table method ',str(e))
                    print(str(e))




        except Exception, e:
            logging.exception('Error in executing check_linkedin_url_with_company_name method ',str(e))
            print(str(e))



    def database_connection_close(self):

             if(self.db_insert!=None):
                 try:
                     self.db_insert.close()
                     logging.exception('Closed database cursor object')
                     print ('Closed database cursor object')


                 except Exception, e:
                     logging.exception('Error in closing  database cursor object',str(e))
                     print(str(e))

             if( self.conn!=None):
                 try:
                    self.conn.close()
                    print('Closed database connection object')
                    logging.exception('Closed database connection object')


                 except Exception, e:
                     logging.exception('Error in closing  database connection object ',str(e))
                     print(str(e))








if __name__ == "__main__":
    optparser = OptionParser()
    optparser.add_option('-n', '--lname',
                         dest='list_name',
                         help='list name',
                         default=None)

    (options, args) = optparser.parse_args()
    list_name = options.list_name
    logging.basicConfig(filename=list_name+'_validation'+'.log', level=logging.INFO, format='%(asctime)s %(message)s')


    linkedin_validate_obj = linkedinUrlValidation()
    linkedin_validate_obj.validate_linkedin(list_name)
    linkedin_validate_obj.database_connection_close()

    logging.exception('linkedin validation process completed')
    print ('linkedin validation process completed')




