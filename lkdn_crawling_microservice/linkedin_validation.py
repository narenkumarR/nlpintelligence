#!/usr/bin/python
__author__ = 'narendra'

from optparse import OptionParser
from fuzzywuzzy import fuzz
from postgres_connect import PostgresConnect
import logging
import tldextract

class linkedinUrlValidation (object):

    def __init__(self):
        pass

    def validate_linkedin(self,list_name,linkedin_cmp_base='crawler.linkedin_company_base '):
        ''' 
        :param list_name: 
        :return:
        '''
        try:
            self.conn = PostgresConnect()
            self.conn.autocommit = True
            self.conn.get_cursor()

            logging.info('connected successfully')
            logging.info('linkedin validation process started')
            print ('linkedin validation process started')
            print (linkedin_cmp_base)


            linkedin_url_query_extract='select id from crawler.list_table where list_name =%s'
            print(linkedin_url_query_extract)

            self.conn.cursor.execute(linkedin_url_query_extract,(list_name,))
            self.conn.commit()
            list_name_info=self.conn.cursor.fetchall()
            logging.info('Fetching list_id from crawler.list_table executed')
            process_list_id= list_name_info[0][0]
            logging.info('List ID Feteched from table :{}'.format(process_list_id))

            self.get_details_to_validate(process_list_id,linkedin_cmp_base)
            self.conn.close_cursor()
            self.conn.close_connection()
        except :
            logging.exception('Issue in executing list_id :{}'.format(list_name))


    def get_details_to_validate (self,list_id,linkedin_cmp_base):
        ''' 
        :param list_id: 
        :return:
        '''
        get_company_website_from_linkedin_and_input_data='select distinct on (company_name)' \
                         ' company_name as linkedin_company_name,website as linkedin_company_website,' \
                   'list_input_additional as input_company_name,list_input as input_company_website,linkedin_url ' \
                         ' from {lkdn_cmp_base} a ' \
                   'join crawler.linkedin_company_redirect_url b on ' \
                         ' (a.linkedin_url = b.redirect_url or a.linkedin_url = b.url) ' \
                         ' join crawler.list_items_urls c on ' \
                   '(b.redirect_url=c.url or b.url=c.url)' \
                   ' and a.list_id=c.list_id join ' \
                   ' crawler.list_items d on c.list_id=d.list_id and c.list_items_id=d.id where a.list_id = %s'.format(lkdn_cmp_base=linkedin_cmp_base)

        self.conn.cursor.execute(get_company_website_from_linkedin_and_input_data,(list_id,))
        self.conn.commit()

        get_complete_info_for_validation = self.conn.cursor.fetchall()

        logging.info('Fetching distinct company names and website from linkedin_company_base table and input '
                    'company name and website from list_item is executed')

        for complete_info_for_validation in get_complete_info_for_validation:
            try:
                linkedin_company_name=  complete_info_for_validation[0]
                linkedin_company_website=  complete_info_for_validation[1]
                input_company_name = complete_info_for_validation[2]
                input_company_website =  complete_info_for_validation[3]
                company_linkedin_url = complete_info_for_validation[4]
                logging.info('Retrieved input company name & website and linkedin company & website for validation process')

                self.check_linkedin_url_with_company_name(linkedin_company_name, linkedin_company_website, 
                                        input_company_name,input_company_website,list_id,company_linkedin_url)
            except:
                logging.exception('Issue in getting company name & website and linkedin company &'
                                  ' website for validation process, check "get_details_to_validate" method.'
                                  'complete_info_for_validation:{}'.format(complete_info_for_validation))

    def check_linkedin_url_with_company_name(self,linkedin_company_name, linkedin_company_website, input_company_name,input_company_website,list_id,company_linkedin_url):
        ''' 
        :param linkedin_company_name: 
        :param linkedin_company_website: 
        :param input_company_name: 
        :param input_company_website: 
        :param list_id: 
        :param company_linkedin_url: 
        :return:
        '''
        linkedin_company_website_str=str(linkedin_company_website)
        linkedin_company_website_str_replace=linkedin_company_website_str.replace("'","")


        linkedin_company_website_domain=tldextract.extract(linkedin_company_website_str_replace).domain


        logging.info('input company name :{} vs linkedin company name:{}, validation starts'.format(input_company_name,linkedin_company_name))
        print(input_company_name,linkedin_company_name)
        fuzzy_score_company_names_compare=fuzz.token_set_ratio(input_company_name,linkedin_company_name)
        logging.info('input company name vs linkedin company name validation completed with score ={}'.format(fuzzy_score_company_names_compare))



        logging.info('input company name :{} vs linkedin company website domain name:{}, validation starts'.format(input_company_name,
                                                                                                         linkedin_company_website_domain))
        print(input_company_name, linkedin_company_website_domain)
        fuzzy_score_company_website_name_compare = fuzz.token_set_ratio(input_company_name, linkedin_company_website_domain)
        logging.info('input company name vs linkedin company website domain name validation completed with score ={}'.format(
        fuzzy_score_company_website_name_compare))


        if(fuzzy_score_company_names_compare >=85) or (fuzzy_score_company_website_name_compare >= 70):
            linkedin_company_name_validate_update=str(linkedin_company_name)
            logging.info('Validation passed linkedin company name from linkedin_company_base table is {}'.format(linkedin_company_name_validate_update))



            try:
                validation_update = "update crawler.linkedin_company_base set isvalid = 1  where list_id = %s and linkedin_url=%s"
                logging.info(
                    'validation updated in linkedin_company_base table with flag as 1 in isValid column')

                self.conn.cursor.execute(validation_update, (list_id, company_linkedin_url,))
                self.conn.commit()
                print(validation_update, (list_id, company_linkedin_url,))

            except Exception as e:
                print(str(e))
                logging.exception('Error in executing validation_update_table method ')
        else:
            try:
                inValidation_update = "update crawler.linkedin_company_base set isvalid = 0  where list_id = %s and linkedin_url=%s and (isvalid!=1 or isvalid is null)"
                logging.info(
                    'inValid records updated in linkedin_company_base table with flag as 0 in isValid column')
                self.conn.cursor.execute(inValidation_update, (list_id,company_linkedin_url,))
                self.conn.commit()
                print(inValidation_update, (list_id, company_linkedin_url,))

            except Exception as e:
                print(str(e))

                logging.exception('Error in executing inValidation_update_table method ')


if __name__ == "__main__":
    optparser = OptionParser()
    optparser.add_option('-n', '--lname',
                         dest='list_name',
                         help='list name',
                         default=None)
    optparser.add_option('-c', '--linkedin_cmp_base',
                         dest='linkedin_company_base_table',
                         help='Either linkedin_company_base or linkedin_company_base_login table for validation',
                         default='crawler.linkedin_company_base')

    (options, args) = optparser.parse_args()
    list_name = options.list_name
    print(list_name)
    linkedin_company_base_login_table_option=options.linkedin_company_base_table
    print(linkedin_company_base_login_table_option)
    logging.basicConfig(filename=list_name+'_validation'+'.log', level=logging.INFO, format='%(asctime)s %(message)s')


    linkedin_validate_obj = linkedinUrlValidation()
    linkedin_validate_obj.validate_linkedin(list_name,linkedin_cmp_base=linkedin_company_base_login_table_option)

    logging.info('linkedin validation process completed')
    print ('linkedin validation process completed')




