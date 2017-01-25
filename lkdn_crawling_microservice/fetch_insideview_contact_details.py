__author__ = 'joswin'
# -*- coding: utf-8 -*-

import pandas as pd
import json
import logging
import requests
import time
import threading

from sqlalchemy import create_engine
from optparse import OptionParser
from Queue import Queue

from fetch_insideview_data import InsideviewFetcher
from postgres_connect import PostgresConnect
from constants import database,host,user,password,designations_column_name

throttler_app_address = 'http://127.0.0.1:5000/'
people_search_url = 'https://api.insideview.com/api/v1/contacts'
contact_fetch_url = 'https://api.insideview.com/api/v1/contact/{contactId}'

class InsideviewContactFetcher(object):

    def __init__(self):
        self.con = PostgresConnect(database_in=database,host_in=host,
                                                    user_in=user,password_in=password)
        self.insideview_fetcher = InsideviewFetcher()

    def main(self,list_name,out_loc,inp_loc=None,desig_loc=None,search_contacts=0,get_emails=0,
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
        logging.basicConfig(filename='logs/insideview_contact_fetching_log_file_{}.log'.format(list_name),
                            level=logging.INFO,format='%(asctime)s %(message)s')
        list_id = self.create_list_id(list_name)
        if inp_loc:
            self.insert_input_to_db(inp_loc,list_id)
        if search_contacts:
            self.search_contact_for_people(list_id)
        if get_emails:
            self.search_contact_for_email(list_id,desig_loc,contact_ids_file_loc=contact_ids_file_loc)
        self.save_results(list_id,list_name,out_loc,search_contacts,get_emails)

    def save_results(self,list_id,list_name,out_loc,search_contacts=0,get_emails=0):
        '''
        :param list_id:
        :return:
        '''
        engine = create_engine('postgresql://{user_name}:{password}@{host}:{port}/{database}'.format(
            user_name=user,password=password,host=host,port='5432',database=database
        ))
        if get_emails:
            query = " select input_filters,c.* from crawler.list_input_insideview_contacts a join " \
                    " crawler.insideview_contact_name_search_res b on a.list_id=b.list_id and a.id=b.list_items_id join " \
                    " crawler.insideview_contact_data c on b.contact_id = c.contact_id " \
                    "where a.list_id = '{}' ".format(list_id)
            df = pd.read_sql_query(query,engine)
            df.to_csv('{}/{}_contacts_emails.csv'.format(out_loc,list_name),index=False,quoting=1,encoding='utf-8')
        if search_contacts:
            query = " select input_filters,b.* from crawler.list_input_insideview_contacts a join " \
                    " crawler.insideview_contact_name_search_res b on a.list_id=b.list_id and a.id=b.list_items_id  " \
                    "where a.list_id = '{}' ".format(list_id)
            df = pd.read_sql_query(query,engine)
            df.to_csv('{}/{}_contacts_search_results.csv'.format(out_loc,list_name),index=False,quoting=1,encoding='utf-8')
        engine.dispose()

    def search_contact_for_email(self,list_id,desig_loc=None,contact_ids_file_loc=None):
        '''
        :param list_id:
        :param desig_loc:
        :return:
        '''
        self.con.get_cursor()
        if contact_ids_file_loc:
            df = pd.read_csv(contact_ids_file_loc)
            contact_ids = list(set(df['contact_ids']))
        else:
            if desig_loc:
                inp_df = pd.read_csv(desig_loc)
                desig_list = list(inp_df[designations_column_name])
            else:
                raise ValueError('Need file with target designations')
            if desig_list:
                desig_list_reg = '\y' + '\y|\y'.join(desig_list) + '\y'
                query = " select distinct a.contact_id from crawler.insideview_contact_name_search_res a " \
                        " left join crawler.insideview_contact_data b on a.contact_id=b.contact_id " \
                        " where b.contact_id is null and  a.list_id = %s and " \
                        " array_to_string(a.titles,',') ~* '{}'".format(desig_list_reg)
            else:
                query = " select distinct a.contact_id from crawler.insideview_contact_name_search_res a " \
                        " left join crawler.insideview_contact_data b on a.contact_id=b.contact_id " \
                        " where b.contact_id is null and a.list_id = %s "
            self.con.cursor.execute(query,(list_id,))
            contact_ids = self.con.cursor.fetchall()
            contact_ids = [i[0] for i in contact_ids]
        self.search_contact_for_email_threaded(contact_ids)
        self.con.commit()
        self.con.close_cursor()

    def search_contact_for_email_threaded(self,contact_ids,n_threads=10):
        '''
        :param contact_ids:
        :param n_threads:
        :return:
        '''
        logging.info('no of new_contact_ids for which insideview fetching done:{}'.format(len(contact_ids)))
        in_queue = Queue(maxsize=0)
        out_queue = Queue(maxsize=0)
        def worker():
            while not in_queue.empty():
                contact_id = in_queue.get()
                res_dic = self.get_contact_details_from_contactid(contact_id)
                if res_dic.get('message'): #throttling reached, need to do this company id again
                    in_queue.put(contact_id)
                    in_queue.task_done()
                    time.sleep(10)
                    continue
                out_queue.put(res_dic)
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
            self.insideview_fetcher.save_contact_info(res_dic)
            out_queue.task_done()
            if out_queue.qsize() < 5:
                time.sleep(10)
        time.sleep(20)
        logging.info('inqueue size:{},outqueue size:{}'.format(in_queue.qsize(),out_queue.qsize()))
        logging.info('finished search_contact_for_email_threaded')

    def get_contact_details_from_contactid(self,contact_id):
        '''
        :param contact_id:
        :return:
        '''
        contact_url = contact_fetch_url.format(contactId=contact_id)
        search_dic = {'url':contact_url}
        r = requests.get(throttler_app_address,params=search_dic)
        res_dic = json.loads(r.text)
        return res_dic

    def search_contact_for_people(self,list_id):
        '''
        :param list_id:
        :return:
        '''
        self.con.get_cursor()
        query = " select distinct on (a.id) a.id,input_filters from crawler.list_input_insideview_contacts a " \
                " left join crawler.insideview_contact_name_search_res b on " \
                " a.list_id=b.list_id and a.id=b.list_items_id where b.list_id is null and a.list_id = %s"
        self.con.cursor.execute(query,(list_id,))
        list_inputs = self.con.cursor.fetchall()
        self.search_contact_for_people_threaded(list_id,list_inputs)
        self.con.commit()
        self.con.close_cursor()

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
                search_results = self.search_insideview_contact(search_dic)
                if search_results.get('message'): #throttling reached, need to do this company id again
                    in_queue.put((list_input_id,search_dic))
                    in_queue.task_done()
                    time.sleep(10)
                    continue
                out_queue.put((list_input_id,search_results))
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

    def search_insideview_contact(self,search_dic):
        ''' search insidevw with the parameters in search_dict
        :param search_dict:
        :return:
        '''
        search_dic['url'] = people_search_url
        search_dic['resultsPerPage'] = 5
        r = requests.get(throttler_app_address,params=search_dic)
        res_dic = json.loads(r.text)
        out_list = res_dic.get('contacts',[])
        return out_list

    def insert_input_to_db(self,inp_loc,list_id):
        ''' load csv. insert fullname to list_input, company website to
        :param inp_loc:
        :param list_name:
        :return:
        '''
        self.con.get_cursor()
        df = pd.read_csv(inp_loc)
        query = " insert into crawler.list_input_insideview_contacts (list_id,input_filters) " \
                " values (%s,%s)"
        for index, row in df.fillna('').iterrows():
            row_dic = dict(row)
            row_dic_not_null = {}
            for key in row_dic:
                if row_dic[key]:
                    row_dic_not_null[key] = row_dic[key]
            self.con.execute(query,(list_id,json.dumps(row_dic_not_null),))
        self.con.commit()
        self.con.close_cursor()

    def create_list_id(self,list_name):
        '''
        :param list_name:
        :return:
        '''
        self.con.get_cursor()
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

    (options, args) = optparser.parse_args()
    list_name = options.list_name
    inp_loc = options.inp_loc
    desig_loc = options.desig_loc
    out_loc = options.out_loc
    search_contacts = options.search_contacts
    get_emails = options.get_emails
    contact_ids_file_loc = options.contact_ids_file
