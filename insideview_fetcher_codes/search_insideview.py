__author__ = 'joswin'
# -*- coding: utf-8 -*-

import json
import logging
import pandas as pd
import time

from postgres_connect import PostgresConnect
from constants import database,host,user,password
from fetch_insideview_from_company_data import API_counter
from fetch_insideview_base import InsideviewDataFetcher
from fetch_insideview_data_utils import InsideviewDataUtil
from sqlalchemy import create_engine
from optparse import OptionParser

class InsideviewSearcher(object):

    def __init__(self,list_name,throttler_app_address = 'http://192.168.3.56:5000/'):
        '''
        :param list_name:
        :param throttler_app_address:
        :return:
        '''
        logging.basicConfig(filename='logs/insideview_filter_search_log_file_{}.log'.format(list_name),
                            level=logging.INFO,format='%(asctime)s %(message)s')
        self.con = PostgresConnect(database_in=database,host_in=host,
                                                    user_in=user,password_in=password)
        self.list_name = list_name
        self.list_id = self.get_list_id(list_name)
        self.api_counter = API_counter(self.list_id)
        self.insideview_fetcher = InsideviewDataFetcher(api_counter=self.api_counter,
                                                        throttler_app_address=throttler_app_address)
        self.data_util = InsideviewDataUtil()

    def main(self,out_loc,inp_loc=None,search_companies_insideview=0,max_no_results=10000):
        '''
        :param list_name:
        :param out_loc:
        :param inp_loc:
        :param desig_loc:
        :param search_contacts:
        :param get_emails:
        :param contact_ids_file_loc:
        :return:
        '''
        logging.info('started main')
        if inp_loc:
            self.insert_input_to_db(inp_loc,self.list_id)
        if search_companies_insideview:
            search_dic = self.load_search_filter(self.list_id)
            try:
                self.search_companies_insideview(self.list_id,search_dic,max_no_results)
            except:
                pass
        self.api_counter.update_list_api_counts()
        self.save_results(self.list_id,self.list_name,out_loc,search_companies_insideview)
        logging.info('completed main')

    def save_results(self,list_id,list_name,out_loc,search_companies_insideview=0):
        '''
        :param list_id:
        :return:
        '''
        logging.info('saving results')
        engine = create_engine('postgresql://{user_name}:{password}@{host}:{port}/{database}'.format(
            user_name=user,password=password,host=host,port='5432',database=database
        ))
        if search_companies_insideview:
            query = " select distinct on (a.new_company_id) * from crawler.insideview_company_name_filter_search a join" \
                    " crawler.insideview_company_details_filter_search b on" \
                    " a.list_id=b.list_id and a.new_company_id=b.new_company_id "\
                    " where a.list_id = '{list_id}' ".format(list_id=list_id)
            df = pd.read_sql_query(query,engine)
            df.to_csv('{}/{}_company_search_results.csv'.format(out_loc,list_name),index=False,quoting=1,encoding='utf-8')
        # getting api hit counts
        query = " select * from crawler.insideview_api_hits where list_id = '{}' ".format(self.list_id)
        df = pd.read_sql_query(query,engine)
        df.to_csv('{}/{}_insideview_api_hits.csv'.format(out_loc,list_name),index=False,quoting=1,encoding='utf-8')
        engine.dispose()

    def search_companies_insideview(self,list_id,filters_dic,max_no_results,results_per_page=500):
        '''
        :param list_id:
        :param filters_dic:
        :return:
        '''
        logging.info('started company search')
        total_results = 0
        if 'page' in filters_dic:
            res_page_no = filters_dic.pop('page')
        else:
            res_page_no = 0
        filters_dic['resultsPerPage'] = results_per_page
        n_errors = 0
        while max_no_results > total_results :
            logging.info('no of results till now:{}'.format(total_results))
            res_page_no += 1
            filters_dic['page'] = res_page_no
            try:
                company_name_res_list = self.insideview_fetcher.search_company_names_in_insideview(filters_dic)
                company_dets_res_list = self.insideview_fetcher.search_company_details_in_insideview(filters_dic)
                n_errors = 0
            except:
                if n_errors > 1:
                    logging.exception('Some error happened. Exiting the search')
                    break
                n_errors +=1
                logging.exception('error happened while getting search results. Try again')
                res_page_no -= 1
                time.sleep(20)
                continue
            if not company_name_res_list and company_dets_res_list: #if both hits not giving any results, exit
                break
            # todo: need to fix below logic. If first hit is good and second hit is blocked, code hits both apis again
            if (company_name_res_list and company_name_res_list[0]=='throttling limit reached') or \
                    (company_dets_res_list and company_dets_res_list[0]=='throttling limit reached'):
                logging.info('server blocked. wait for 20 seconds')
                res_page_no -= 1
                time.sleep(20)
                continue
            self.data_util.save_company_name_filter_search_res(company_name_res_list,list_id)
            self.data_util.save_company_details_filter_search_res(company_dets_res_list,list_id)
            self.api_counter.company_name_filter_search_hits += 1
            self.api_counter.company_details_filter_search_hits += 1
            # if no of results are less than results_per_page, stop
            if len(company_name_res_list) < results_per_page or len(company_dets_res_list) < results_per_page:
                break
            total_results += min(len(company_name_res_list),len(company_dets_res_list))
        logging.info('completed company search')

    def load_search_filter(self,list_id):
        self.con.get_cursor()
        query = "select search_parameteres from crawler.list_input_insideview_search where list_id=%s and active='t' "
        self.con.execute(query,(list_id,))
        res = self.con.cursor.fetchall()
        if not res:
            raise ValueError('crawler.list_input_insideview_search corresponding to this list_name is empty')
        search_dic = json.loads(res[0][0])
        self.con.close_cursor()
        return search_dic

    def save_search_filter(self,list_id,filters_dic):
        self.con.get_cursor()
        query = " update crawler.list_input_insideview_search set active='f' where list_id=%s"
        self.con.execute(query,(list_id,))
        self.con.commit()
        query = "insert into crawler.list_input_insideview_search (list_id,search_type,search_parameteres,active) " \
                "values (%s,%s,%s,%s)"
        self.con.execute(query,(list_id,'company_search_iv',json.dumps(filters_dic),True,))
        self.con.commit()
        self.con.close_cursor()

    def insert_input_to_db(self,inp_loc,list_id):
        ''' load csv. insert fullname to list_input, company website to
        :param inp_loc:
        :param list_name:
        :return:
        '''
        logging.info('insert_input_to_db started')
        filters_dic = self.data_util.gen_filters_dic(inp_loc)
        self.save_search_filter(list_id,filters_dic)
        logging.info('insert_input_to_db finished')

    def get_list_id(self,list_name):
        '''
        :param list_name:
        :return:
        '''
        self.con.get_cursor()
        self.con.execute("select id from crawler.list_table_insideview_search where list_name = %s",(list_name,))
        res = self.con.cursor.fetchall()
        if not res:
            query = " insert into crawler.list_table_insideview_search (list_name) values (%s) on conflict do nothing"
            self.con.execute(query,(list_name,))
            self.con.execute("select id from crawler.list_table_insideview_search where list_name = %s",(list_name,))
            res = self.con.cursor.fetchall()
        list_id = res[0][0]
        self.con.commit()
        self.con.close_cursor()
        return list_id

if __name__ == "__main__":
    optparser = OptionParser()
    optparser.add_option('-n', '--name',
                         dest='list_name',
                         help='name of the list',
                         default=None)
    optparser.add_option('-i', '--inp_loc',
                         dest='inp_loc',
                         help='location of csv containing company names and websites',
                         default=None)
    optparser.add_option('-o', '--out_loc',
                         dest='out_loc',
                         help='folder location of output files',
                         default=None)
    optparser.add_option('--search_companies_insideview',
                         dest='search_companies_insideview',
                         help='search for companies from insideview using filters if 1',
                         default=0,type='int')
    optparser.add_option('--max_no_results',
                         dest='max_no_results',
                         help='maximum no of results needed',
                         default=10000,type='int')
    optparser.add_option('--throttler_address',
                         dest='throttler_address',
                         help='throttler ip address',
                         default='http://127.0.0.1:5000/')

    (options, args) = optparser.parse_args()
    list_name = options.list_name
    inp_loc = options.inp_loc
    out_loc = options.out_loc
    search_companies_insideview = options.search_companies_insideview
    max_no_results = options.max_no_results
    throttler_address = options.throttler_address
    fetcher = InsideviewSearcher(list_name=list_name,throttler_app_address=throttler_address)
    fetcher.main(out_loc=out_loc,inp_loc=inp_loc,search_companies_insideview=search_companies_insideview,
                 max_no_results=max_no_results)