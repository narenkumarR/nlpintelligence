__author__ = 'joswin'

import logging
from optparse import OptionParser
from postgres_connect import PostgresConnect
from company_linkedin_url_extractor.company_extractor import CompanyLinkedinURLExtractorMulti
# from sqlalchemy import create_engine

from constants import problematic_urls_file

list_items_table = 'crawler.list_items_urls'
urls_to_crawl_priority_table = 'crawler.linkedin_company_urls_to_crawl_priority'

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
            "where a.list_id = %s and b.list_id is null"
        self.con.execute(query,(list_id,))
        in_list = self.con.cursor.fetchall()
        if not in_list:
            return
        tmp_dic = {}
        for list_items_id,list_input,list_input_additional in in_list:
            tmp_dic[list_items_id] = (list_input,list_input_additional)
        logging.info('started process for finding linkedin url for {} companies'.format(len(tmp_dic.keys())))
        out_dict = self.cc.get_linkedin_url_multi(tmp_dic,n_threads=1,time_out=100)
        out_dict1 = {}
        prob_dict = {}
        for i in out_dict:
            if out_dict[i][1] ==0 or not out_dict[i][0]:
                prob_dict[i] = tmp_dic[i]
            else:
                out_dict1[i] = out_dict[i][0]
        # save out_dict1 into urls to list_items_urls
        # save prob_dict as csv
        logging.info('found linkedin urls for {} items. No of items with problem: {}'.format(len(out_dict1.keys()),len(prob_dict.keys())))
        out_list = []
        for i in out_dict1:
            out_list.append((list_id,i,out_dict1[i],))
        if out_list:
            records_list_template = ','.join(['%s']*len(out_list))
            insert_query = "INSERT INTO {} (list_id,list_items_id,url) VALUES {} "\
                    "ON CONFLICT DO NOTHING".format(list_items_table,records_list_template)
            self.con.cursor.execute(insert_query, out_list)
            self.con.commit()
            # insert data into priority table so the crawling process will start running
            query = "insert into {} (url,list_id,list_items_url_id) select url,list_id,id as list_items_url_id "\
                " from {} where list_id = %s on conflict do nothing".format(urls_to_crawl_priority_table,list_items_table)
            self.con.cursor.execute(query,(list_id,))
            self.con.commit()
        self.con.close_cursor()
        if prob_dict:
            with open(problematic_urls_file,'w') as f:
                f.write('Following Company names had problems:\n')
                for i in prob_dict:
                    f.write('{}\n'.format(prob_dict[i]))
        logging.info('completed linkedin url extraction process')

if __name__ == "__main__":
    optparser = OptionParser()
    optparser.add_option('-f', '--file',
                         dest='csv_company',
                         help='location of csv with company names',
                         default=None)
    # optparser.add_option('-d', '--designations',
    #                      dest='desig_loc',
    #                      help='location of csv containing target designations',
    #                      default=None)
    (options, args) = optparser.parse_args()
    csv_company = options.csv_company
    # desig_loc = options.desig_loc

    extractor = LkdnUrlExtrMain()
    extractor.run_main(csv_company)

