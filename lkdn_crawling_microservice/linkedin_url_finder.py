__author__ = 'joswin'

import logging
import re
from optparse import OptionParser
from postgres_connect import PostgresConnect
from linkedin_company_url_extraction_micro_service.company_linkedin_url_extractor.company_extractor import CompanyLinkedinURLExtractorMulti
# from sqlalchemy import create_engine

from constants import company_common_reg,user_agents

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

    def run_main(self,list_id=None,threads=1):
        '''
        :param list_id:
        :return:
        '''
        logging.info('started linkedin url extraction process')
        if list_id is None:
            raise ValueError('Need list_id input')
        self.con.get_cursor()
        query = "select a.id as list_items_id, a.list_input,a.list_input_additional "\
                "from crawler.list_items a " \
            "where a.list_id = %s and " \
                " ( a.url_extraction_tried != 1 ) "
        # " left join crawler.list_items_urls b on a.list_id=b.list_id and a.id=b.list_items_id "\
        # or b.url is null
        self.con.execute(query,(list_id,))
        in_list = self.con.cursor.fetchall()
        # also extract company name for companies for which employee details is null. For these companies, searching
        # in ddg
        # query = " select distinct on (company_name) list_items_id,'' list_input,company_name as list_input_additional " \
        #         " from crawler.linkedin_company_base a join crawler.list_items_urls b on a.list_id=b.list_id and " \
        #         " a.list_items_url_id = b.id where a.list_id=%s and employee_details is null "
        # self.con.execute(query,(list_id,))
        # in_list.extend(self.con.cursor.fetchall())
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
        for key,linkedin_url,conf,people_urls in self.cc.get_linkedin_url_multi(tmp_dic,n_threads=threads,time_out=100):
            # key is basically list_items_id from list_items table
            # logging.info('linkedin_url_finder: data from url extractor, key:{},linkedin_url:{},conf:{}'.format(key,linkedin_url,conf))
            if linkedin_url:
                linkedin_url = re.split(r'\?',linkedin_url)[0]
                linkedin_url = re.split(r'/careers(/|$)|/employee-insights(/|$)|/jobs(/|$)|/activity(/|$)|'
                                        r'analytics(/|$)|insights(/|$)|edit(/|$)|product(s)?(/|$)|statistics(/|$)|'
                                        r'blogs(/|$)|news(/|$)|tweets(/|$)|notifications(/|$)',linkedin_url)[0]
                linkedin_url = 'https://www.linkedin.com/'+re.split('linkedin\.com/',linkedin_url)[-1]
                if not linkedin_url:
                    continue
                # insert into list_items_urls table
                insert_query = "INSERT INTO {} (list_id,list_items_id,url)  " \
                               " select %s,%s,%s where not exists " \
                                " (select 1 from {} where list_id =%s " \
                                " and list_items_id=%s)".format(list_items_urls_table,list_items_urls_table)
                self.con.cursor.execute(insert_query, (list_id,key,linkedin_url,list_id,key,))
                self.con.commit()
                query = " update {list_items_table} set url_extraction_tried = 1 " \
                        " where list_id = %s and id = %s  ".format(
                    list_items_table='crawler.list_items'
                )
                self.con.cursor.execute(query,(list_id,key,))
                self.con.commit()
                # get list_items_url_id
                query = " select id from {} where list_id = %s and list_items_id = %s ".format(list_items_urls_table)
                self.con.cursor.execute(query,(list_id,key,))
                tmp = self.con.cursor.fetchall()
                if not tmp:
                    continue
                list_items_url_id = tmp[0][0]
                # following part not needed because it will be taken care of in table updation
                # if re.search(r'/company/|/companies/',linkedin_url):
                #     query = "insert into {} (url,list_id,list_items_url_id) values (%s,%s,%s) " \
                #         " on conflict do nothing".format(urls_to_crawl_priority_table_company)
                #     self.con.cursor.execute(query,(linkedin_url,list_id,list_items_url_id,))
                # elif re.search(r'/in/|/pub/',linkedin_url):
                #     query = "insert into {} (url,list_id,list_items_url_id) values (%s,%s,%s) " \
                #         " on conflict do nothing".format(urls_to_crawl_priority_table_people)
                #     self.con.cursor.execute(query,(linkedin_url,list_id,list_items_url_id,))
                if people_urls:
                    people_urls = people_urls[:min(3,len(people_urls))]
                    records_list_template = ','.join(['%s']*len(people_urls))
                    insert_query = "INSERT INTO {} (url,list_id,list_items_url_id) VALUES {} "\
                     "ON CONFLICT DO NOTHING".format(urls_to_crawl_priority_table_people,records_list_template)
                    urls_to_crawl1 = [(i,list_id,list_items_url_id,) for i in people_urls]
                    self.con.cursor.execute(insert_query, urls_to_crawl1)
                    # logging.info('people list for url: {} ,no urls: {}'.format(linkedin_url,len(people_urls)))
                self.con.commit()
            else:
                # # why was this done. need to investigate
                # delete_query = "DELETE FROM {} WHERE list_id = %s AND id = %s".format('crawler.list_items')
                # self.con.execute(delete_query,(list_id,key,))
                # self.con.commit()
                # with open(problematic_urls_file,'a') as f:
                #     f.write('{}\n'.format(tmp_dic[key]))
                query = " update {list_items_table} set url_extraction_tried = 1 " \
                        " where list_id = %s and id = %s  ".format(
                    list_items_table='crawler.list_items'
                )
                self.con.cursor.execute(query,(list_id,key,))
                self.con.commit()
        self.con.close_cursor()
        logging.info('completed url extraction process')

    def run_command(self,list_name,threads=1):
        '''
        :param list_name:
        :return:
        '''
        logging.basicConfig(filename='log_file_linkedin_url_extractor_{}.log'.format(list_name),
                            level=logging.INFO,format='%(asctime)s %(message)s')
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
        self.run_main(list_id,threads=threads)

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
    optparser.add_option('-v', '--visible',
                         dest='visible',
                         help='visible',
                         default=0,type='int')
    optparser.add_option('-t', '--threads',
                         dest='threads',
                         help='no of threads',
                         default=1,type='int')
    (options, args) = optparser.parse_args()
    list_name = options.list_name
    visible = options.visible
    threads = options.threads
    # desig_loc = options.desig_loc

    extractor = LkdnUrlExtrMain(visible=visible)
    extractor.run_command(list_name,threads=threads)

