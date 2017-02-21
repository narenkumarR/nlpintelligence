__author__ = 'joswin'
# -*- coding: utf-8 -*-

import pandas as pd
import json
import logging
import time
import threading

from sqlalchemy import create_engine
from optparse import OptionParser
from Queue import Queue

from postgres_connect import PostgresConnect
from constants import database,host,user,password,designations_column_name
from fetch_insideview_base import InsideviewDataFetcher
from fetch_insideview_from_company_data import API_counter
from fetch_insideview_data_utils import InsideviewDataUtil

class InsideviewContactFetcher(object):

    def __init__(self,list_name,throttler_app_address = 'http://192.168.3.56:5000/'):
        '''
        :param list_name:
        :param throttler_app_address:
        :return:
        '''
        logging.basicConfig(filename='logs/insideview_contact_fetching_log_file_{}.log'.format(list_name),
                            level=logging.INFO,format='%(asctime)s %(message)s')
        self.con = PostgresConnect(database_in=database,host_in=host,
                                                    user_in=user,password_in=password)
        self.list_name = list_name
        self.list_id = self.get_list_id(list_name)
        self.api_counter = API_counter(self.list_id)
        self.insideview_fetcher = InsideviewDataFetcher(api_counter=self.api_counter,
                                                        throttler_app_address=throttler_app_address)
        self.data_util = InsideviewDataUtil()

    def main(self,out_loc,inp_loc=None,desig_loc=None,search_contacts=0,get_emails=0,
             contact_ids_file_loc=None):
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
        if search_contacts:
            self.search_contact_for_people(self.list_id)
        if get_emails:
            self.search_contact_for_email(self.list_id,desig_loc,contact_ids_file_loc=contact_ids_file_loc)
        self.api_counter.update_list_api_counts()
        self.save_results(self.list_id,self.list_name,out_loc,search_contacts,get_emails)
        logging.info('completed main')

    def save_results(self,list_id,list_name,out_loc,search_contacts=0,get_emails=0):
        '''
        :param list_id:
        :return:
        '''
        logging.info('saving results')
        engine = create_engine('postgresql://{user_name}:{password}@{host}:{port}/{database}'.format(
            user_name=user,password=password,host=host,port='5432',database=database
        ))
        if get_emails:
            query = " (select c.* from " \
                    " crawler.insideview_contact_name_search_res b join " \
                    " crawler.insideview_contact_data c on b.contact_id = c.contact_id " \
                    "where b.list_id = '{list_id}' )" \
                    "union " \
                    "( select b.* from crawler.insideview_contact_search_res a join crawler.insideview_contact_data b " \
                    " on a.email_md5_hash=b.email_md5_hash where a.active = 't' and " \
                    " a.list_id = '{list_id}' )".format(list_id=list_id)
            df = pd.read_sql_query(query,engine)
            df.to_csv('{}/{}_contacts_emails.csv'.format(out_loc,list_name),index=False,quoting=1,encoding='utf-8')
        if search_contacts:
            query = " select a.full_name as input_name,a.first_name as input_first_name,a.last_name as input_last_name," \
                    "a.company_id as input_company_id,b.* from crawler.insideview_contact_search_res a left join " \
                    " crawler.insideview_contact_name_search_res b on a.list_id=b.list_id and b.input_name_id=a.people_id  " \
                    "where a.list_id = '{}' ".format(list_id)
            df = pd.read_sql_query(query,engine)
            df.to_csv('{}/{}_contacts_search_results.csv'.format(out_loc,list_name),index=False,quoting=1,encoding='utf-8')
        # getting api hit counts
        query = " select * from crawler.insideview_api_hits where list_id = '{}' ".format(self.list_id)
        df = pd.read_sql_query(query,engine)
        df.to_csv('{}/{}_insideview_api_hits.csv'.format(out_loc,list_name),index=False,quoting=1,encoding='utf-8')
        engine.dispose()

    def search_contact_for_email(self,list_id,desig_loc=None,contact_ids_file_loc=None,n_threads=10):
        '''
        :param list_id:
        :param desig_loc:
        :return:
        '''
        self.con.get_cursor()
        logging.info('started fetch_people_details_from_company_ids_ppl_details')
        query = " select distinct company_id from crawler.insideview_contact_search_res where list_id = %s"
        self.con.cursor.execute(query,(list_id,))
        comp_ids = self.con.cursor.fetchall()
        comp_ids = [i[0] for i in comp_ids]
        if not comp_ids:
            raise ValueError('Need company ids')
        if desig_loc:
            desig_list = self.data_util.get_designations(desig_loc)
        else:
            desig_list = []
        contact_ids = self.data_util.get_contact_ids_for_email_find(list_id,comp_ids,max_res_per_company=1000,
                                                                    desig_list=desig_list,
                                                      contact_ids_file_loc=contact_ids_file_loc)
        if contact_ids:
            print('no of contacts for which emails will be fetched:{}'.format(len(contact_ids)))
            self.fetch_people_details_from_contact_ids(contact_ids,n_threads)
        self.con.commit()
        self.con.close_cursor()

    def fetch_people_details_from_contact_ids(self,contact_ids,n_threads=10):
        '''
        :param new_contact_ids:
        :param retrieve_comp_dets:
        :param n_threads:
        :return:
        '''
        logging.info('no of contact_ids for which insideview fetching to be done:{}'.format(len(contact_ids)))
        in_queue = Queue(maxsize=0)
        out_queue = Queue(maxsize=0)
        def worker():
            while not in_queue.empty():
                contact_id = in_queue.get()
                res_dic = self.insideview_fetcher.get_contact_details_from_contactid(contact_id)
                if res_dic.get('message') in ['request throttled by insideview','1000 per 5 minute']: #throttling reached, need to do this company id again
                    in_queue.put(contact_id)
                    in_queue.task_done()
                    time.sleep(10)
                    continue
                elif res_dic.get('message'):
                    raise ValueError('Error happened. {}'.format(res_dic))
                out_queue.put(res_dic)
                self.api_counter.contact_fetch_hits += 1
                in_queue.task_done()
        for contact_id in contact_ids:
            in_queue.put(contact_id)
        for i in range(n_threads):
            worker_tmp = threading.Thread(target=worker)
            worker_tmp.setDaemon(True)
            worker_tmp.start()
        time.sleep(20)
        while not out_queue.empty() or not in_queue.empty():
            logging.info('inqueue size:{},outqueue size:{}'.format(in_queue.qsize(),out_queue.qsize()))
            res_dic = out_queue.get()
            self.data_util.save_contact_info(res_dic)
            out_queue.task_done()

            if out_queue.qsize() < 5:
                time.sleep(10)
        time.sleep(20)
        logging.info('inqueue size:{},outqueue size:{}'.format(in_queue.qsize(),out_queue.qsize()))
        logging.info('finished fetch_people_details_from_contact_ids')

    def search_contact_for_people(self,list_id,n_threads=10):
        '''
        :param list_id:
        :return:
        '''
        self.con.get_cursor()
        # query = " select distinct on (a.id) a.id,input_filters from crawler.list_input_insideview_contacts a " \
        #         " left join crawler.insideview_contact_name_search_res b on " \
        #         " a.list_id=b.list_id and a.id=b.list_items_id where b.list_id is null and a.list_id = %s"
        # self.con.cursor.execute(query,(list_id,))
        # list_inputs = self.con.cursor.fetchall()
        # self.search_contact_for_people_threaded(list_id,list_inputs)
        query = " select distinct company_id from crawler.insideview_contact_search_res where list_id = %s"
        self.con.cursor.execute(query,(list_id,))
        comp_ids = self.con.cursor.fetchall()
        comp_ids = [i[0] for i in comp_ids]
        if not comp_ids:
            raise ValueError('Need company ids')
        ppl_details = self.data_util.get_people_details_for_email_find(list_id=list_id,comp_ids=comp_ids,
                                            desig_list=[],people_details_file=None)
        if ppl_details:
            self.search_for_matching_people_from_ppl_details(list_id,ppl_details,n_threads=n_threads)
        self.con.commit()
        self.con.close_cursor()

    def search_for_matching_people_from_ppl_details(self,list_id,ppl_details,n_threads=10):
        '''
        :param ppl_details:
        :param n_threads:
        :return:
        '''
        logging.info('no of ppl_details for which people search to be done:{}'.format(len(ppl_details)))
        in_queue_tuple_order_keys = ['companyId','firstName','lastName','fullName']
        in_queue = Queue(maxsize=0)
        out_queue = Queue(maxsize=0)
        def worker():
            while not in_queue.empty():
                dets_tuple,person_id = in_queue.get()
                # logging.info('search for person_id:{},dets_tuple:{}'.format(person_id,dets_tuple))
                search_dic = {}
                for key,value in zip(in_queue_tuple_order_keys,dets_tuple):
                    if value:
                        search_dic[key] = value
                if not search_dic:
                    out_queue.put(([],person_id))
                    in_queue.task_done()
                    continue
                # logging.info('search_dic:{}'.format(search_dic))
                res_dic = self.insideview_fetcher.search_insideview_contact(search_dic)
                logging.info('contact name search:')
                logging.info('search_dic:{}'.format(search_dic))
                logging.info('res_dic:{}'.format(res_dic))
                self.api_counter.people_search_hits += 1
                # logging.info('res_dic:{}'.format(res_dic))
                if res_dic.get('message') in ['request throttled by insideview','1000 per 5 minute']: #throttling reached, need to do this company id again
                    in_queue.put((dets_tuple,person_id))
                    in_queue.task_done()
                    time.sleep(10)
                    continue
                elif res_dic.get('message'):
                    raise ValueError('Error happened. {}'.format(res_dic))
                # logging.info('search result person_id:{}, res_dic contacts:{}'.format(person_id,res_dic.get('contacts',[])))
                out_queue.put((res_dic.get('contacts',[]),person_id))
                in_queue.task_done()
        for dets_tuple in ppl_details:
            person_dets,person_id = dets_tuple[:-1],dets_tuple[-1] #last item is the id
            in_queue.put((person_dets,person_id))
        for i in range(n_threads):
            worker_tmp = threading.Thread(target=worker)
            worker_tmp.setDaemon(True)
            worker_tmp.start()
        time.sleep(20)
        while not out_queue.empty() or not in_queue.empty():
            logging.info('inqueue size:{},outqueue size:{}'.format(in_queue.qsize(),out_queue.qsize()))
            res_list,person_id = out_queue.get()
            # logging.info('save to db person_id:{},res_list:{}'.format(person_id,res_list))
            self.data_util.save_contact_search_res_single(list_id,res_list,person_id)
            out_queue.task_done()
            if out_queue.qsize() < 5:
                time.sleep(10)
        time.sleep(20)
        logging.info('inqueue size:{},outqueue size:{}'.format(in_queue.qsize(),out_queue.qsize()))
        logging.info('finished search_for_matching_people_from_ppl_details')

    def search_contact_for_people_threaded(self,list_id,list_inputs,n_threads=10):
        '''
        :param list_id:
        :param list_inputs:
        :param n_threads:
        :return:
        '''
        logging.info('no of new_contact_ids for which insideview fetching done:{}'.format(len(list_inputs)))
        in_queue = Queue(maxsize=0)
        out_queue = Queue(maxsize=0)
        def worker():
            while not in_queue.empty():
                list_input_id,search_dic = in_queue.get()
                search_dic = json.loads(search_dic)
                search_dic['isEmailRequired'] = True
                res_dic = self.insideview_fetcher.search_insideview_contact(search_dic)
                if search_results.get('message') in ['request throttled by insideview','1000 per 5 minute']: #throttling reached, need to do this company id again
                    in_queue.put((list_input_id,search_dic))
                    in_queue.task_done()
                    time.sleep(10)
                    continue
                elif search_results.get('message'):
                    raise ValueError('Error happened. {}'.format(res_dic))
                out_list = res_dic.get('contacts',[])
                out_queue.put((list_input_id,out_list))
                in_queue.task_done()
        for list_input_id,search_dic in list_inputs:
            in_queue.put((list_input_id,search_dic))
        for i in range(n_threads):
            worker_tmp = threading.Thread(target=worker)
            worker_tmp.setDaemon(True)
            worker_tmp.start()
        time.sleep(20)
        while not out_queue.empty() or not in_queue.empty():
            logging.info('inqueue size:{},outqueue size:{}'.format(in_queue.qsize(),out_queue.qsize()))
            list_input_id,search_results = out_queue.get()
            self.save_contact_search_res_single(list_id,list_input_id,search_results)
            out_queue.task_done()
            if out_queue.qsize() < 5:
                time.sleep(10)
        time.sleep(20)
        logging.info('inqueue size:{},outqueue size:{}'.format(in_queue.qsize(),out_queue.qsize()))
        logging.info('finished search_contact_for_people_threaded')

    def save_contact_search_res_single(self,list_id,list_items_id,res_list):
        '''
        :param list_id:
        :param list_items_id:
        :param res_list:
        :return:
        '''
        self.con.get_cursor()
        res_to_insert = [
            (list_id,list_items_id,res.get('firstName',None),res.get('middleName',None),res.get('lastName',None),
                res.get('contactId',None),res.get('companyId',None),res.get('companyName',None),res.get('titles',None),
                res.get('active',None),res.get('hasEmail',None),res.get('hasPhone',None),res.get('peopleId',None)
            )
            for res in res_list
        ]
        records_list_template = ','.join(['%s'] * len(res_to_insert))
        insert_query = 'insert into crawler.insideview_contact_name_search_res ' \
                       ' (list_id, list_items_id,first_name ,middle_name ,last_name ,contact_id ,company_id , ' \
                       ' company_name ,titles ,active ,has_email,has_phone ,people_id)' \
                       ' values {}'.format(records_list_template)
        self.con.execute(insert_query, res_to_insert)
        self.con.commit()
        self.con.close_cursor()

    def insert_input_to_db(self,inp_loc,list_id):
        ''' load csv. insert fullname to list_input, company website to
        :param inp_loc:
        :param list_name:
        :return:
        '''
        logging.info('insert_input_to_db started')
        self.con.get_cursor()
        df = pd.read_csv(inp_loc)
        # query = " insert into crawler.list_input_insideview_contacts (list_id,input_filters) " \
        #         " values (%s,%s)"
        # for index, row in df.fillna('').iterrows():
        #     row_dic = dict(row)
        #     row_dic_not_null = {}
        #     for key in row_dic:
        #         if row_dic[key]:
        #             row_dic_not_null[key] = row_dic[key]
        #     self.con.execute(query,(list_id,json.dumps(row_dic_not_null),))
        res_list = df.to_dict('records')
        self.data_util.save_contacts_seach_res(list_id,res_list)
        self.con.commit()
        self.con.close_cursor()
        logging.info('insert_input_to_db finished')

    def get_list_id(self,list_name):
        '''
        :param list_name:
        :return:
        '''
        self.con.get_cursor()
        self.con.execute("select id from crawler.list_table_insideview_contacts where list_name = %s",(list_name,))
        res = self.con.cursor.fetchall()
        if not res:
            query = " insert into crawler.list_table_insideview_contacts (list_name) values (%s) on conflict do nothing"
            self.con.execute(query,(list_name,))
            self.con.execute("select id from crawler.list_table_insideview_contacts where list_name = %s",(list_name,))
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
    optparser.add_option('-d', '--desig_loc',
                         dest='desig_loc',
                         help='folder location of designations file',
                         default=None)
    optparser.add_option('--search_contacts',
                         dest='search_contacts',
                         help='search for contact details if 1',
                         default=0,type='int')
    optparser.add_option('--get_emails',
                         dest='get_emails',
                         help='get emails for the contacts if 1',
                         default=0,type='int')
    optparser.add_option('--contact_ids_file',
                         dest='contact_ids_file',
                         help='location of contact ids. emails will be fetched for these contacts only if present',
                         default=None)
    optparser.add_option('--throttler_address',
                         dest='throttler_address',
                         help='throttler ip address',
                         default='http://127.0.0.1:5000/')

    (options, args) = optparser.parse_args()
    list_name = options.list_name
    inp_loc = options.inp_loc
    desig_loc = options.desig_loc
    out_loc = options.out_loc
    search_contacts = options.search_contacts
    get_emails = options.get_emails
    contact_ids_file_loc = options.contact_ids_file
    throttler_address = options.throttler_address

    fetcher = InsideviewContactFetcher(list_name=list_name,throttler_app_address=throttler_address)
    fetcher.main(out_loc=out_loc,inp_loc=inp_loc,desig_loc=desig_loc,search_contacts=search_contacts,
                 get_emails=get_emails,contact_ids_file_loc=contact_ids_file_loc)