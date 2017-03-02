__author__ = 'joswin'
# -*- coding: utf-8 -*-

import logging
import pandas as pd
import time
from sqlalchemy import create_engine
from optparse import OptionParser
from postgres_connect import PostgresConnect
import datetime

import threading
from Queue import Queue

from constants import database,host,user,password
from fetch_insideview_base import InsideviewDataFetcher
from fetch_insideview_data_utils import InsideviewDataUtil
from api_counter import API_counter

def chunker(seq, size):
    return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))

company_name_field,website_field,country_field,state_field,city_field = 'company_name','website','country','state','city'

class InsideviewCompanyFetcher(object):
    '''
    '''
    def __init__(self,list_name,throttler_app_address = 'http://192.168.3.56:5000/',
                 create_if_not_present=False):
        self.con = PostgresConnect(database_in=database,host_in=host,
                                                    user_in=user,password_in=password)
        self.list_name = list_name
        self.list_id = self.get_list_id(list_name,create_if_not_present=create_if_not_present)
        self.api_counter = API_counter(self.list_id)
        self.insideview_fetcher = InsideviewDataFetcher(self.api_counter,throttler_app_address=throttler_app_address)
        self.data_util = InsideviewDataUtil()

    def fetch_data_csv_input(self,out_loc,filters_loc,inp_loc=None,max_comps_to_try=0,get_contacts=1,
                             get_comp_dets_sep=0,max_res_per_company=3,remove_comps_in_lkdn_table=0,
                                   find_new_comps_only=0,desig_loc=None,comp_contries_loc=None,search_contacts=1,
                             comp_ids_to_find_contacts_file_loc=None,search_companies=1,
                             new_contact_ids_file_loc=None,people_details_file=None,
                             contact_ids_file_loc=None):
        '''
        :param inp_loc: input csv file. columns website,company_name
        :param out_loc:location of folder(should not end with "/"
        :param filters_loc: ;location of filter file (filter_key,filter_value format)
        :param max_comps_to_try: no of companies needed
        :param get_contacts: get contact details and emails if true
        :param get_comp_dets_sep: get company details also while searching for contacts(works only if get_contacts True)
        :param max_res_per_company: max number of email searches to be done for each company
        :param remove_comps_in_lkdn_table: if True, remove the companies already present in the linkedin tables from processing.
                otherwise they will be processed
        :param find_new_comps_only: if True, try to find contacts for companies for whom we don't have enough contact
                details. set this to False when we are trying new filters. eg: suppose  we generate a list of people with
                filterA. When running for same list again with filterA, set this flag as True. If we are running
                with different filter, filterB, set this flag as False. Running again with filterB, set this flag as True.
                This is useful in reducing target api hits. Setting wrongly will affect in more api hits.
        :param desig_list: list of designations
        :param comp_contries_loc: location of files with country names targeted. column name: countries
        :param search_contacts: should we search for contacts
        :param comp_ids_to_find_contacts_file_loc: only for these companies, contacts will be searched. column name
                should be company_ids
        :param search_companies: should we search for companies
        :param new_contact_ids_file_loc: location of new contact ids for which emails need to be fetched
        :param people_details_file: 'company_id','first_name','last_name','full_name','people_id'
        :return:
        '''
        logging.basicConfig(filename='logs/insideview_fetching_log_file_{}.log'.format(self.list_name),
                            level=logging.INFO,format='%(asctime)s %(message)s')
        logging.info('starting process for list_name:{}, list_id:{}'.format(self.list_name,self.list_id))
        if out_loc[-1] == '/':
            out_loc = out_loc[:-1]
        self.con.get_cursor()
        if inp_loc:
            self.upload_company_url_list(inp_loc,self.list_id)
        # get list_id
        try:
            self.fetch_data_crawler_process(list_id=self.list_id,filters_loc=filters_loc,max_comps_to_try=max_comps_to_try,
                                            get_contacts=get_contacts,get_comp_dets_sep=get_comp_dets_sep,
                                            max_res_per_company=max_res_per_company,
                                            remove_comps_in_lkdn_table=remove_comps_in_lkdn_table,
                                            find_new_comps_only=find_new_comps_only,desig_loc=desig_loc,
                                            comp_contries_loc=comp_contries_loc,search_contacts=search_contacts,
                                            comp_ids_to_find_contacts_file_loc=comp_ids_to_find_contacts_file_loc,
                                            search_companies=search_companies,
                                            new_contact_ids_file_loc=new_contact_ids_file_loc,
                                            contact_ids_file_loc=contact_ids_file_loc,
                                            people_details_file=people_details_file)
        except:
            logging.exception('Error happened in the process')
        self.api_counter.update_list_api_counts()
        # get all data in csv
        engine = create_engine('postgresql://{user_name}:{password}@{host}:{port}/{database}'.format(
            user_name=user,password=password,host=host,port='5432',database=database
        ))
        file_name = self.list_name#+'_'+str(datetime.datetime.now())
        if search_companies:
            # get all companies in the search
            query = "select b.company_name as input_name,b.website as input_website,b.country as input_country," \
                    "a.name,a.company_id,a.city,a.state,a.country " \
                    "from crawler.insideview_company_search_res a left join crawler.list_items_insideview_companies b on" \
                    " a.list_items_id = b.id where" \
                    " a.list_id='{}'".format(self.list_id)
            df = pd.read_sql_query(query,engine)
            df.to_csv('{}/{}_company_search_results.csv'.format(out_loc,file_name),index=False,quoting=1,encoding='utf-8')
        if desig_loc:
            desig_list = self.data_util.get_designations(desig_loc)
            desig_list_reg = '\y' + '\y|\y'.join(desig_list) + '\y'
        else:
            desig_list = []
            desig_list_reg = ''
        if comp_ids_to_find_contacts_file_loc:
            # if company ids are given specifically in file, give results only for them
            comp_ids = self.data_util.get_companies_for_contact_search(self.list_id,comp_contries_loc,0,
                                                             comp_ids_to_find_contacts_file_loc)
            comp_ids_query = '(' + ','.join([str(i) for i in comp_ids]) + ')'
        else:
            comp_ids,comp_ids_query = [],''
        if contact_ids_file_loc:
            contact_ids = self.data_util.get_contact_ids_for_email_find(self.list_id,comp_ids,desig_list,
                                                      contact_ids_file_loc=contact_ids_file_loc)
            contact_ids_query =  '(' + ','.join([str(i) for i in contact_ids]) + ')'
        else:
            contact_ids,contact_ids_query = [],''
        # if get_comp_dets_sep :
        # get details for all companies
        query = "select distinct b.*,c.company_name as input_name,c.website as input_website," \
                "c.country as input_country from crawler.insideview_company_search_res a join" \
                " crawler.insideview_company_details_contact_search b on a.company_id=b.company_id " \
                " left join crawler.list_items_insideview_companies c on a.list_items_id = c.id " \
                " where a.list_id='{}'".format(self.list_id)
        if comp_ids:
            query = query + ' and a.company_id in {}'.format(comp_ids_query)
        df = pd.read_sql_query(query,engine)
        df.to_csv('{}/{}_company_details.csv'.format(out_loc,file_name),index=False,quoting=1,encoding='utf-8')
        # if search_contacts or get_contacts:
        # get all contact search result
        query = "select * from crawler.insideview_contact_search_res where list_id = '{}'".format(self.list_id)
        if comp_ids:
            query = query + ' and company_id in {}'.format(comp_ids_query)
        df = pd.read_sql_query(query,engine)
        df.to_csv('{}/{}_contact_search_result.csv'.format(out_loc,file_name),index=False,quoting=1,encoding='utf-8')
        query = "select * from crawler.insideview_contact_name_search_res where list_id = '{}'".format(self.list_id)
        if comp_ids:
            query = query + ' and company_id in {}'.format(comp_ids_query)
        df = pd.read_sql_query(query,engine)
        df.to_csv('{}/{}_contact_search_result_people_matches.csv'.format(out_loc,file_name),index=False,quoting=1,encoding='utf-8')
        # if search_contacts or get_contacts:
        # get all emails
        query = " select * from (" \
                "(select distinct b.* from crawler.insideview_contact_name_search_res a join " \
                " crawler.insideview_contact_data b on a.contact_id=b.contact_id " \
                " where a.list_id = '{list_id}')" \
                " union" \
                "(select distinct b.* from crawler.insideview_contact_search_res a join " \
                " crawler.insideview_contact_data b on a.email_md5_hash=b.email_md5_hash " \
                " where a.list_id = '{list_id}')" \
                ")x where email is not null ".format(list_id=self.list_id)
        if desig_list:
            query = query + " and array_to_string(x.titles,',') ~* '{}' ".format(desig_list_reg)
        if comp_ids:
            query = query + ' and x.company_id in {}'.format(comp_ids_query)
        if contact_ids:
            query = query + ' and x.contact_id in {} '.format(contact_ids_query)
        df = pd.read_sql_query(query,engine)
        df.to_csv('{}/{}_contact_email_data.csv'.format(out_loc,file_name),index=False,quoting=1,encoding='utf-8')
        # getting api hit counts
        query = " select * from crawler.insideview_api_hits where list_id = '{}' ".format(self.list_id)
        df = pd.read_sql_query(query,engine)
        df.to_csv('{}/{}_insideview_api_hits.csv'.format(out_loc,file_name),index=False,quoting=1,encoding='utf-8')
        engine.dispose()

    
    
    def fetch_data_crawler_process(self,list_id,filters_loc,max_comps_to_try=0,get_contacts=1,
                                   get_comp_dets_sep=0,max_res_per_company=3,remove_comps_in_lkdn_table=1,
                                   find_new_comps_only=0,desig_loc=None,comp_contries_loc=None,search_contacts=0,
                                   comp_ids_to_find_contacts_file_loc=None,search_companies=1,
                                   new_contact_ids_file_loc=None,people_details_file=None,
                                   contact_ids_file_loc=None,n_threads=10):
        '''
        '''
        logging.info('starting the insideview fetch')
        self.con.get_cursor()
        # todo : add option to give countries - set the list id as null for comps not matching in insideview_company_search_res table
        # get the filters present in filters_loc
        filters_dic = self.data_util.gen_filters_dic(filters_loc)
        desig_list = self.data_util.get_designations(desig_loc)
        logging.info('loaded filters dictionary')
        if search_companies:
            # get the company details(name and website) which needs to be fetched from insideview
            comp_input_dets = self.data_util.get_dets_for_insideview_fetch(list_id,remove_comps_in_lkdn_table,max_comps_to_try)
            logging.info('no of companies to search : {}'.format(len(comp_input_dets)))
            # running inside view search for each company and saving the output to table
            if comp_input_dets:
                self.company_search_insideview_multi(comp_input_dets,list_id)
            logging.info('completed company search')
        # get all company ids which need to be searched for contacts from table
        comp_ids = self.data_util.get_companies_for_contact_search(list_id,comp_contries_loc,0,
                                                         comp_ids_to_find_contacts_file_loc)
        logging.info('no of companies for which contact search is to be done:{}'.format(len(comp_ids)))
        # if get_comp_dets_sep is True, search for each comp_id in insideview and save the company details
        if get_comp_dets_sep:
            comp_ids_not_present = self.data_util.get_company_ids_missing(list_id,comp_ids)
            if comp_ids_not_present:
                self.get_save_company_details_from_insideview_compid_input(comp_ids_not_present)
            logging.info('saved the company details for each company')
        # todo : we can add option to search companies using all the data fetched here
        if search_contacts:
            # search for people from these comp_ids
            logging.info('starting contact search')
            comp_ids_to_search = self.data_util.get_companies_for_contact_search(list_id,comp_contries_loc,0,
                                                         comp_ids_to_find_contacts_file_loc)
            if comp_ids_to_search:
                self.search_contacts_from_company_ids(list_id,company_ids=comp_ids_to_search,max_res_per_company=max_res_per_company,
                                                                      **filters_dic)
                # logging.info('no of contacts got from the contact search: {}'.format(len(contacts_list)))
                # if contacts_list:
                #     self.data_util.save_contacts_seach_res(list_id,contacts_list)
                #     logging.info('saved contact search results into table')
        # if no need to get contacts, return, else continue
        if get_contacts:
            logging.info('searching with name and other details to get contact_id')
            ppl_details = self.data_util.get_people_details_for_email_find(list_id=list_id,comp_ids=comp_ids,
                                            desig_list=desig_list,people_details_file=people_details_file)
            logging.info('No of people for whom contact_id search will be done:{}'.format(len(ppl_details)))
            if len(ppl_details)>5000 and not people_details_file and not desig_list:
                raise ValueError('More than 5000 people matched for email fetching. Reduce the number')
            if ppl_details:
                self.search_for_matching_people_from_ppl_details(list_id,ppl_details,n_threads=n_threads)
            # fetching emails
            self.fetch_people_details_from_company_ids_from_contactids(list_id,comp_ids,max_res_per_company=max_res_per_company,
                                                desig_list=desig_list,
                                                contact_ids_file_loc=contact_ids_file_loc,
                                                new_contact_ids_file_loc=new_contact_ids_file_loc
                                                )
        self.con.close_cursor()
        self.con.close_connection()
        logging.info('completed the insideview fetch process')

    def fetch_people_details_from_company_ids_from_contactids(self,list_id,comp_ids,max_res_per_company=5,
                                              desig_list=[],n_threads=10,
                                              contact_ids_file_loc=None,new_contact_ids_file_loc=None):
        '''
        :param list_id:
        :param comp_ids:
        :param max_res_per_company:
        :param retrieve_comp_dets:
        :param desig_list:
        :param n_threads:
        :param new_contact_ids_file_loc:
        :return:
        '''
        logging.info('started fetch_people_details_from_company_ids_ppl_details')
        contact_ids = self.data_util.get_contact_ids_for_email_find(list_id,comp_ids,max_res_per_company=max_res_per_company,
                                                                    desig_list=desig_list,
                                                      contact_ids_file_loc=contact_ids_file_loc)
        print('No of contact ids for which email will be found:{}'.format(len(contact_ids)))
        logging.info('No of contact ids for which email will be found:{}'.format(len(contact_ids)))
        if contact_ids:
            self.fetch_people_details_from_contact_ids(contact_ids,n_threads)
        if new_contact_ids_file_loc:#if new contactids provided, directly fetch emails (might be costlier)
            new_contact_ids = self.data_util.get_new_contact_ids_to_fetch(list_id,comp_ids,
                                                   max_res_per_company=max_res_per_company,desig_list=desig_list,
                                                   new_contact_ids_file_loc=new_contact_ids_file_loc)
            if len(new_contact_ids)>5000:
                raise ValueError('Too many emails to fetch. Tighten the condition')
            if new_contact_ids:
                logging.info('getting emails for new contact ids')
                self.fetch_people_details_from_newcontact_ids(new_contact_ids,retrieve_comp_dets= 0)

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

    def fetch_people_details_from_newcontact_ids(self,new_contact_ids,retrieve_comp_dets=0,n_threads=10):
        '''
        :param new_contact_ids:
        :param retrieve_comp_dets:
        :param n_threads:
        :return:
        '''
        logging.info('no of new_contact_ids for which insideview fetching done:{}'.format(len(new_contact_ids)))
        in_queue = Queue(maxsize=0)
        out_queue = Queue(maxsize=0)
        def worker():
            while not in_queue.empty():
                new_contact_id = in_queue.get()
                res_dic = self.insideview_fetcher.get_contact_details_from_newcontact_id(new_contact_id,retrieve_comp_dets)
                if res_dic.get('message') in ['request throttled by insideview','1000 per 5 minute']: #throttling reached, need to do this company id again
                    in_queue.put(new_contact_id)
                    in_queue.task_done()
                    time.sleep(10)
                    continue
                elif res_dic.get('message'):
                    raise ValueError('Error happened. {}'.format(res_dic))
                out_queue.put(res_dic)
                self.api_counter.newcontact_email_hits += 1
                in_queue.task_done()
        for new_contact_id in new_contact_ids:
            in_queue.put(new_contact_id)
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
        logging.info('finished fetch_people_details_from_company_ids_crawler_process')

    def fetch_people_details_from_company_ids_new_contactids(self,list_id,comp_ids,max_res_per_company=3,
                                              retrieve_comp_dets=0,desig_list=[],n_threads=10,
                                              new_contact_ids_file_loc=None):
        '''Find newcontact_ids of people and find emails using the new contact id
        :param list_id:
        :param comp_ids:
        :param max_res_per_company: max number of email searches to be done for each company
        :param filters_dic:
        :return:
        '''
        logging.info('started fetch_people_details_from_company_ids_crawler_process')
        new_contact_ids = self.data_util.get_new_contactids_for_email_find(list_id=list_id,comp_ids=comp_ids,
                                            max_res_per_company=max_res_per_company,desig_list=desig_list,
                                            new_contact_ids_file_loc=new_contact_ids_file_loc)
        if not new_contact_ids:
            logging.info('no contact ids to fetch from insideview')
            return
        else:
            self.fetch_people_details_from_newcontact_ids(new_contact_ids,retrieve_comp_dets=retrieve_comp_dets,
                                                          n_threads=n_threads)

    def company_search_insideview_multi(self,comp_input_dets,list_id,n_threads=10):
        '''search in insideview and save the results from the input list of companies
        '''
        logging.info('started company_search_insideview_multi')
        in_queue = Queue(maxsize=0)
        out_queue = Queue(maxsize=0)
        def worker():
            while not in_queue.empty():
                company_name,website,country,state,city,list_items_id = in_queue.get()
                # logging.info('trying for company: {},{}'.format(comp_website,comp_name))
                # if comp_website and comp_name:
                #     comp_search_results = self.search_company_single(comp_name,comp_website,max_no_results=50)
                if website and 'linkedin.com/compan' not in website.lower() and \
                                'facebook.com/' not in website.lower():
                    comp_search_results = self.insideview_fetcher.search_company_single(None,website,country,state,city,
                                                                                        max_no_results=50)
                else:
                    comp_search_results = []
                if not comp_search_results and company_name:
                    if 'linkedin.com/compan' in website.lower() or \
                                'facebook.com/' in website.lower():
                        comp_search_results = self.insideview_fetcher.search_company_single(company_name,None,country,
                                                                                        state,city,max_no_results=10)
                    else:
                        comp_search_results = self.insideview_fetcher.search_company_single(company_name,website,
                                                                                country,state,city,max_no_results=10)
                if comp_search_results and comp_search_results[0] == 'throttling limit reached': #checking if throttling happened
                    logging.info('throttling limit reached: comp_search_results:{}'.format(comp_search_results))
                    in_queue.put((company_name,website,country,state,city,list_items_id))
                    in_queue.task_done()
                    time.sleep(10)
                    continue
                elif not comp_search_results:
                    comp_search_results = [{}] #no search result. pass an empty dic so that nulls will be populated to tables
                out_queue.put((list_items_id,comp_search_results))
                in_queue.task_done()
                # logging.info('completed for company: {},{}'.format(comp_website,comp_name))
        logging.info('starting the threads')
        for inp_tuple in comp_input_dets:
            in_queue.put(inp_tuple)
        for i in range(n_threads):
            worker_tmp = threading.Thread(target=worker)
            worker_tmp.setDaemon(True)
            worker_tmp.start()
        logging.info('sleeping for 60 seconds')
        time.sleep(60)
        while not out_queue.empty() or not in_queue.empty():
            logging.info('inqueue size:{},outqueue size:{}'.format(in_queue.qsize(),out_queue.qsize()))
            list_items_id,comp_search_results = out_queue.get()
            # logging.info('saving to database : {},{}'.format(list_items_id,comp_search_results))
            # if comp_search_results: #not needed as when running again, this causes this company to run again
            self.data_util.save_company_search_res_single(list_id,list_items_id,comp_search_results)
            # logging.info('completed saving to database : {},{}'.format(list_items_id,comp_search_results))
            out_queue.task_done()
            if out_queue.qsize() < 5:
                time.sleep(10)
        # sleep for 20 seconds and save again
        time.sleep(20)
        logging.info('inqueue size:{},outqueue size:{}'.format(in_queue.qsize(),out_queue.qsize()))
        logging.info('completed company_search_insideview_multi')

    def get_save_company_details_from_insideview_compid_input(self,comp_ids_not_present,n_threads=10):
        ''' '''
        logging.info('no of comp_ids to fetch from insideview:{}'.format(len(comp_ids_not_present)))
        in_queue = Queue(maxsize=0)
        out_queue = Queue(maxsize=0)
        def worker():
            while not in_queue.empty():
                comp_id = in_queue.get()
                comp_dets_dic = self.insideview_fetcher.get_company_details_from_id(comp_id)
                if comp_dets_dic.get('message') in ['request throttled by insideview','1000 per 5 minute']: #throttling reached, need to do this company id again
                    in_queue.put(comp_id)
                    in_queue.task_done()
                    time.sleep(10)
                    continue
                elif comp_dets_dic.get('message'):
                    raise ValueError('Error happened. {}'.format(comp_dets_dic))
                out_queue.put(comp_dets_dic)
                in_queue.task_done()
        for comp_id in comp_ids_not_present:
            in_queue.put(comp_id)
        for i in range(n_threads):
            worker_tmp = threading.Thread(target=worker)
            worker_tmp.setDaemon(True)
            worker_tmp.start()
        logging.info('sleeping for 20 seconds')
        time.sleep(20)
        while not out_queue.empty() or not in_queue.empty():
            logging.info('inqueue size:{},outqueue size:{}'.format(in_queue.qsize(),out_queue.qsize()))
            comp_dets_dic = out_queue.get()
            self.data_util.save_company_dets_dic_input(comp_dets_dic)
            out_queue.task_done()
            self.api_counter.company_details_hits += 1
            if out_queue.qsize() < 5:
                time.sleep(10)
        time.sleep(20)
        logging.info('inqueue size:{},outqueue size:{}'.format(in_queue.qsize(),out_queue.qsize()))
        logging.info('completed get_save_company_details_from_insideview_listinput')

    def search_contacts_from_company_ids(self,list_id,company_ids,max_res_per_company=5,**filters_dic):
        '''
        :param company_ids:
        :param kwargs:
        :return:
        '''
        # max_no_results=len(comp_ids)*(max_res_per_company+5)
        company_ids = [i for i in company_ids if i] # sometimes none coming
        # all_contacts = []
        no_comps_to_process_single_iter = 500/(max_res_per_company) #
        for comp_ids in chunker(company_ids,no_comps_to_process_single_iter):
            comp_id_str = ','.join([str(i) for i in comp_ids])
            filters_dic['companyIdsToInclude'] = comp_id_str
            contacts_list = self.insideview_fetcher.search_contacts(max_no_results=no_comps_to_process_single_iter*50,**filters_dic)
            logging.info('no contact search results :{}'.format(len(contacts_list)))
            if contacts_list:
                self.data_util.save_contacts_seach_res(list_id,contacts_list)

    def upload_company_url_list(self,csv_loc=None,list_id=None):
        '''
        :param csv_loc:
        :param list_name:
        :return:
        '''
        if csv_loc is None:
            raise ValueError('give location of csv with company details . Col names '\
                    'should be {},{},{}'.format(company_name_field,website_field))
        if list_id is None:
            raise ValueError('Need list name')
        # url_df = pd.read_csv(csv_loc)
        url_df = pd.read_csv(csv_loc,sep=None)
        url_df.columns = [i.decode('ascii','ignore') for i in url_df.columns]
        url_df = url_df.fillna('')
        self.upload_company_url_list_df_inp(url_df,list_id)

    def upload_company_url_list_df_inp(self,url_df,list_id):
        '''
        :param url_df:
        :param list_name:
        :return:
        '''#company_name_field,website_field,country_field,state_field,city_field = 'company_name','website','country','state','city'
        company_dets = [tuple([list_id])+tuple(row[[company_name_field,website_field,country_field,state_field,city_field]].fillna(''))
                        for ind,row in url_df.iterrows()]
        self.con.get_cursor()
        # prob with getting correct list_items_id while inserting- insert the name to list_item table
        if company_dets:
            records_list_template = ','.join(['%s']*len(company_dets))
            insert_query = "INSERT INTO {} (list_id,company_name,website,country,state,city) VALUES {} "\
                            "ON CONFLICT DO NOTHING".format('crawler.list_items_insideview_companies',records_list_template)
            self.con.cursor.execute(insert_query, company_dets)
            self.con.commit()
        self.con.close_cursor()
        if 'company_id' in url_df:
            self.insert_company_id_direct(url_df,list_id)

    def insert_company_id_direct(self,url_df,list_id):
        '''
        :param url_df:
        :param list_id:
        :return:
        '''
        self.con.get_cursor()
        query = " select id,company_name,website,country,state,city from {} where list_id=%s" \
                "".format('crawler.list_items_insideview_companies')
        self.con.cursor.execute(query,(list_id,))
        res = self.con.cursor.fetchall()
        list_items_df = pd.DataFrame(res,columns=['list_items_id',company_name_field,website_field,country_field,
                                                  state_field,city_field])
        comb_df = pd.merge(url_df,list_items_df)
        dets = [tuple([list_id])+tuple(row[['list_items_id','company_id',company_name_field,country_field,state_field,city_field]].fillna(''))
                        for ind,row in comb_df.iterrows()]
        if dets:
            records_list_template = ','.join(['%s']*len(dets))
            insert_query = "INSERT INTO {} (list_id,list_items_id,company_id,name,country,state,city) VALUES {} "\
                            "ON CONFLICT DO NOTHING".format('crawler.insideview_company_search_res',records_list_template)
            self.con.cursor.execute(insert_query, dets)
            self.con.commit()
        self.con.close_cursor()

    def get_list_id(self,list_name,create_if_not_present=False):
        self.con.get_cursor()
        self.con.execute("select id from crawler.list_table_insideview_companies where list_name = %s",(list_name,))
        res = self.con.cursor.fetchall()
        # con.close_cursor()
        if not res:
            if create_if_not_present:
                # raise ValueError('the list name given do not have any records')
                query = " insert into crawler.list_table_insideview_companies (list_name) values (%s) "
                self.con.execute(query,(list_name,))
                self.con.commit()
                self.con.execute("select id from crawler.list_table_insideview_companies where list_name = %s",(list_name,))
                res = self.con.cursor.fetchall()
            else:
                raise ValueError('list name not present')
        list_id = res[0][0]
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
    optparser.add_option('-f', '--filter_loc',
                         dest='filter_loc',
                         help='location of file containing filter variables',
                         default=None)
    optparser.add_option('-o', '--out_loc',
                         dest='out_loc',
                         help='folder location of output files',
                         default=None)
    optparser.add_option('--max_comps_to_try',
                         dest='max_comps_to_try',
                         help='maximum no of companies which will be processed. if 0, all will be processed',
                         type='int',
                         default=0)
    optparser.add_option('--get_emails',
                         dest='get_contacts',
                         help='get contact details and emails if 1',
                         default=0,type='int')
    optparser.add_option('--max_res_per_company',
                         dest='max_res_per_company',
                         help='max number of email searches to be done for each company',
                         default=5,type='int')
    optparser.add_option('--get_comp_dets_sep',
                         dest='get_comp_dets_sep',
                         help='if 1,get company details also while searching for contacts(works only if get_contacts is 1)',
                         default=0,type='int')
    optparser.add_option('--remove_comps_in_lkdn_table',
                         dest='remove_comps_in_lkdn_table',
                         help="if 1, try to find contacts only for companies for whom we don't have data in linkedin company table",
                         default=0,type='int')
    optparser.add_option('--find_new_comps_only',
                         dest='find_new_comps_only',
                         help="if 1, try to find contacts for companies for whom we don't have enough contact details",
                         default=1,type='int')
    optparser.add_option('-d', '--designations',
                         dest='desig_loc',
                         help='location of csv containing target designations',
                         default=None)
    optparser.add_option('--throttler_address',
                         dest='throttler_address',
                         help='throttler ip address',
                         default='http://127.0.0.1:5000/')
    optparser.add_option('--comp_contries_loc',
                         dest='comp_contries_loc',
                         help='location of countries. only companies from those companies will be filtered',
                         type='str',
                         default=None)
    optparser.add_option('--search_contacts',
                         dest='search_contacts',
                         help='if 1 search contacts',
                         default=0,type='int')
    optparser.add_option('--search_companies',
                         dest='search_companies',
                         help='if 1 search companies',
                         default=0,type='int')
    optparser.add_option('--company_ids_file',
                         dest='company_ids_file',
                         help='location of company ids. only contacts from these companies will be searched',
                         default=None)
    optparser.add_option('--contact_ids_file',
                         dest='contact_ids_file',
                         help='location of contact ids. emails will be fetched for these contacts only',
                         default=None)
    optparser.add_option('--new_contact_ids_file',
                         dest='new_contact_ids_file',
                         help='location of new contact ids. emails will be fetched for these contacts only',
                         default=None)
    optparser.add_option('--people_details_file',
                         dest='people_details_file',
                         help='location of people_details_file. emails will be fetched for these contacts only',
                         default=None)
    (options, args) = optparser.parse_args()
    list_name = options.list_name
    inp_loc = options.inp_loc
    filter_loc = options.filter_loc
    out_loc = options.out_loc
    max_comps_to_try = options.max_comps_to_try
    get_contacts = options.get_contacts
    get_comp_dets_sep = options.get_comp_dets_sep
    max_res_per_company = options.max_res_per_company
    remove_comps_in_lkdn_table = options.remove_comps_in_lkdn_table
    find_new_comps_only = options.find_new_comps_only
    desig_loc = options.desig_loc
    throttler_address = options.throttler_address
    comp_contries_loc = options.comp_contries_loc
    search_contacts = options.search_contacts
    search_companies = options.search_companies
    comp_ids_to_find_contacts_file_loc = options.company_ids_file
    contact_ids_file_loc = options.contact_ids_file
    people_details_file = options.people_details_file
    new_contact_ids_file = people_details_file
    # new_contact_ids_file = contact_ids_file_loc #single file, give contact_id and new_contact_id columns
    # checks
    if not list_name:
        raise ValueError('need list name to run')
    if not out_loc:
        raise ValueError('need output folder location')
    if inp_loc or comp_ids_to_find_contacts_file_loc or people_details_file or contact_ids_file_loc:
        create_list_id_if_not_present = True
    else:
        create_list_id_if_not_present = False
    fetcher = InsideviewCompanyFetcher(list_name,throttler_app_address=throttler_address,
                                       create_if_not_present=create_list_id_if_not_present)
    fetcher.fetch_data_csv_input(inp_loc=inp_loc,out_loc=out_loc,filters_loc=filter_loc,
                                 max_comps_to_try=max_comps_to_try,get_contacts=get_contacts,
                                 get_comp_dets_sep=get_comp_dets_sep,max_res_per_company=max_res_per_company,
                                 remove_comps_in_lkdn_table=remove_comps_in_lkdn_table,
                                 find_new_comps_only=find_new_comps_only,desig_loc=desig_loc,
                                 comp_contries_loc=comp_contries_loc,search_contacts=search_contacts,
                                 comp_ids_to_find_contacts_file_loc=comp_ids_to_find_contacts_file_loc,
                                 search_companies=search_companies,contact_ids_file_loc=contact_ids_file_loc,
                                 new_contact_ids_file_loc=None,people_details_file=people_details_file)
