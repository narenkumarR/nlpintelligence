__author__ = 'joswin'

import logging
from optparse import OptionParser
from postgres_connect import PostgresConnect
from linkedin_company_url_extraction_micro_service.company_linkedin_url_extractor.company_extractor import CompanyLinkedinURLExtractorMulti
# from sqlalchemy import create_engine

from constants import problematic_urls_file

list_items_urls_table = 'crawler.list_items_urls'
urls_to_crawl_priority_table_company = 'crawler.linkedin_company_urls_to_crawl_priority'
urls_to_crawl_priority_table_people = 'crawler.linkedin_people_urls_to_crawl_priority'

class LkdnUrlExtrMain(object):
    '''
    '''
    def __init__(self,visible=False):
        '''
        :return:
        '''
        self.cc = CompanyLinkedinURLExtractorMulti(visible=visible)
        self.con = PostgresConnect()

    def run_main(self,list_id=None):
        '''
        :param list_id:
        :return:
        '''
        logging.info('started linkedin url extraction process')
        if list_id is None:
            raise ValueError('Need list_id input')
        self.con.get_cursor()
        query = "select a.id as list_items_id, a.list_input,a.list_input_additional "\
                "from crawler.list_items a left join crawler.list_items_urls b "\
                "on a.list_id=b.list_id and a.id = b.list_items_id "\
            "where a.list_id = %s and (b.list_id is null or b.url = '')"
        self.con.execute(query,(list_id,))
        in_list = self.con.cursor.fetchall()
        if not in_list:
            return
        # logging.info('companies for which url needs to be find : {}'.format(in_list))
        tmp_dic = {}
        for list_items_id,list_input,list_input_additional in in_list:
            tmp_dic[list_items_id] = (list_input,list_input_additional)
        logging.info('started process for finding linkedin url for {} companies'.format(len(tmp_dic.keys())))
        # with open(problematic_urls_file,'w') as f:
        #     f.write('Following Company names had problems:\n')
        # iterating through results
        for key,linkedin_url,conf in self.cc.get_linkedin_url_multi(tmp_dic,n_threads=1,time_out=100):
            logging.info('linkedin_url_finder: data from url extractor, key:{},linkedin_url:{},conf:{}'.format(key,linkedin_url,conf))
            if linkedin_url:
                # insert into list_items_urls table
                insert_query = "INSERT INTO {} (list_id,list_items_id,url) VALUES (%s,%s,split_part(split_part(split_part(%s,'?trk',1),'/careers',1),'/employee-insights',1)) "\
                    "ON CONFLICT DO NOTHING".format(list_items_urls_table)
                self.con.cursor.execute(insert_query, (list_id,key,linkedin_url,))
                self.con.commit()
                query = "insert into {} (url,list_id,list_items_url_id) select url,list_id,id as list_items_url_id "\
                " from {} where list_id = %s and url like '%%/company/%%' on conflict do nothing".format(urls_to_crawl_priority_table_company,list_items_urls_table)
                self.con.cursor.execute(query,(list_id,))
                self.con.commit()
                query = "insert into {} (url,list_id,list_items_url_id) select url,list_id,id as list_items_url_id "\
                " from {} where list_id = %s and (url like '%%/in/%%' or url like '%%/pub/%%') on conflict do nothing".format(urls_to_crawl_priority_table_people,list_items_urls_table)
                self.con.cursor.execute(query,(list_id,))
                self.con.commit()
            else:
                # # why was this done. need to investigate
                # delete_query = "DELETE FROM {} WHERE list_id = %s AND id = %s".format('crawler.list_items')
                # self.con.execute(delete_query,(list_id,key,))
                # self.con.commit()
                with open(problematic_urls_file,'a') as f:
                    f.write('{}\n'.format(tmp_dic[key]))
        self.con.close_cursor()
        logging.info('completed url extraction process')

    def run_command(self,list_name):
        '''
        :param list_name:
        :return:
        '''
        logging.basicConfig(filename='log_file_linkedin_url_extractor.log', level=logging.INFO,format='%(asctime)s %(message)s')
        logging.info('started linkedin url extraction process from command line')
        if list_name is None:
            raise ValueError('Need list_name input')
        self.con.get_cursor()
        query = 'select id from crawler.list_table where list_name = %s'
        self.con.cursor.execute(query,(list_name,))
        res_list = self.con.cursor.fetchall()
        if not res_list:
            raise ValueError('List name not present in database')
        else:
            list_id = res_list[0][0]
        self.con.close_cursor()
        self.run_main(list_id)

if __name__ == "__main__":
    optparser = OptionParser()
    optparser.add_option('-n', '--lname',
                         dest='list_name',
                         help='list name',
                         default=None)
    # optparser.add_option('-d', '--designations',
    #                      dest='desig_loc',
    #                      help='location of csv containing target designations',
    #                      default=None)
    (options, args) = optparser.parse_args()
    list_name = options.list_name
    # desig_loc = options.desig_loc

    extractor = LkdnUrlExtrMain()
    extractor.run_command(list_name)

