__author__ = 'joswin'
# -*- coding: utf-8 -*-

import csv
import logging
import json
import hashlib
import requests
import pandas as pd
import time
from sqlalchemy import create_engine
from optparse import OptionParser
from postgres_connect import PostgresConnect
from random import shuffle

from company_linkedin_urls_manual_insert import upload_url_list
from constants import database,host,user,password,designations_column_name

def chunker(seq, size):
    return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))

throttler_app_address = 'http://127.0.0.1:5000/'
company_search_url = 'https://api.insideview.com/api/v1/companies'
contact_search_url = 'https://api.insideview.com/api/v1/target/contacts'
contact_details_url = 'https://api.insideview.com/api/v1/target/contact/{newcontactId}'
company_details_url = 'https://api.insideview.com/api/v1/company/{companyId}'
company_tech_profile_url = 'https://api.insideview.com/api/v1/company/{companyId}/techProfile'

class InsideviewFetcher(object):
    '''
    '''
    def __init__(self):
        self.con = PostgresConnect(database_in=database,host_in=host,
                                                    user_in=user,password_in=password)

    def fetch_data_csv_input(self,list_name,out_loc,filters_loc,inp_loc=None,max_comps_to_try=300,get_contacts=1,
                             get_comp_dets_sep=1,max_res_per_company=3,remove_comps_in_lkdn_table=1,
                                   find_new_contacts_only=1,desig_list=[],comp_contries_loc=None):
        '''
        :param list_name: a name for the list
        :param inp_loc: input csv file. columns website,company_name
        :param out_loc:location of folder(should not end with "/"
        :param filters_loc: ;location of filter file (filter_key,filter_value format)
        :param max_comps_to_try: no of companies needed
        :param get_contacts: get contact details and emails if true
        :param get_comp_dets_sep: get company details also while searching for contacts(works only if get_contacts True)
        :param max_res_per_company: max number of email searches to be done for each company
        :param remove_comps_in_lkdn_table: if True, remove the companies already present in the linkedin tables from processing.
                otherwise they will be processed
        :param find_new_contacts_only: if True, try to find contacts for companies for whom we don't have enough contact
                details. set this to False when we are trying new filters. eg: suppose  we generate a list of people with
                filterA. When running for same list again with filterA, set this flag as True. If we are running
                with different filter, filterB, set this flag as False. Running again with filterB, set this flag as True.
                This is useful in reducing target api hits. Setting wrongly will affect in more api hits.
        :param desig_list: list of designations
        :param comp_contries_loc: country names
        :return:
        '''
        # logging.basicConfig(filename='logs/insideview_fetching_log_file_{}.log'.format(list_name),
        #                     level=logging.INFO,format='%(asctime)s %(message)s')
        if out_loc[-1] == '/':
            out_loc = out_loc[:-1]
        self.con.get_cursor()
        if inp_loc:
            upload_url_list(inp_loc,list_name)
        # get list_id
        query = 'select id from crawler.list_table where list_name=%s'
        self.con.execute(query,(list_name,))
        tmp = self.con.cursor.fetchall()
        self.con.close_cursor()
        list_id = tmp[0][0]
        self.fetch_data_crawler_process(list_id=list_id,filters_loc=filters_loc,max_comps_to_try=max_comps_to_try,
                                        get_contacts=get_contacts,get_comp_dets_sep=get_comp_dets_sep,
                                        max_res_per_company=max_res_per_company,
                                        remove_comps_in_lkdn_table=remove_comps_in_lkdn_table,
                                        find_new_contacts_only=find_new_contacts_only,desig_list=desig_list,
                                        comp_contries_loc=comp_contries_loc)
        # get all data in csv
        engine = create_engine('postgresql://{user_name}:{password}@{host}:{port}/{database}'.format(
            user_name=user,password=password,host=host,port='5432',database=database
        ))
        # get all companies in the search
        query = "select b.list_input as input_website,b.list_input_additional as input_company_name," \
                "a.name,a.company_id,a.city,a.state,a.country " \
                "from crawler.insideview_company_search_res a join crawler.list_items b on a.list_items_id = b.id where" \
                " a.list_id='{}'".format(list_id)
        df = pd.read_sql_query(query,engine)
        df.to_csv('{}/{}_company_search_results.csv'.format(out_loc,list_name),index=False,quoting=1,encoding='utf-8')
        # get details for all companies
        query = "select distinct b.* from crawler.insideview_company_search_res a join" \
                " crawler.insideview_company_details_contact_search b on a.company_id=b.company_id" \
                " where a.list_id='{}'".format(list_id)
        df = pd.read_sql_query(query,engine)
        df.to_csv('{}/{}_company_details.csv'.format(out_loc,list_name),index=False,quoting=1,encoding='utf-8')
        # get all contact search result
        query = "select * from crawler.insideview_contact_search_res where list_id = '{}'".format(list_id)
        df = pd.read_sql_query(query,engine)
        df.to_csv('{}/{}_contact_search_result.csv'.format(out_loc,list_name),index=False,quoting=1,encoding='utf-8')
        # get all emails
        query = "select distinct b.* from crawler.insideview_contact_search_res a join " \
                " crawler.insideview_contact_data b on a.email_md5_hash=b.email_md5_hash " \
                " where a.list_id = '{}'".format(list_id)
        df = pd.read_sql_query(query,engine)
        df.to_csv('{}/{}_contact_email_data.csv'.format(out_loc,list_name),index=False,quoting=1,encoding='utf-8')
        engine.dispose()

    
    
    def fetch_data_crawler_process(self,list_id,filters_loc,max_comps_to_try=0,get_contacts=1,
                                   get_comp_dets_sep=1,max_res_per_company=3,remove_comps_in_lkdn_table=1,
                                   find_new_contacts_only=1,desig_list=[],comp_contries_loc=None):
        '''
        :param list_id:
        :param filters_loc: location of filter file
        :param max_comps_to_try: no of companies needed
        :param get_contacts: get contact details and emails if true
        :param get_comp_dets_sep: get company details also while searching for contacts(works only if get_contacts True)
        :param remove_comps_in_lkdn_table: if True, remove the companies already present in the linkedin tables from processing.
                otherwise they will be processed
        :param find_new_contacts_only: if True, try to find contacts for companies for whom we don't have enough contact
                details. set this to False when we are trying new filters. eg: suppose  we generate a list of people with
                filterA. When running for same list again with filterA, set this flag as True. If we are running
                with different filter, filterB, set this flag as False. Running again with filterB, set this flag as True.
                This is useful in reducing target api hits. Setting wrongly will affect in more api hits.
        :param desig_list: list of designations
        :param comp_contries_loc: country names
        :return:
        '''
        self.con.get_cursor()
        # todo : add option to give countries - set the list id as null for comps not matching in insideview_company_search_res table
        # get the filters present in filters_loc
        filters_dic = self.gen_filters_dic(filters_loc)
        # get the company details(name and website) which needs to be fetched from insideview
        comp_input_dets = self.get_dets_for_insideview_fetch(list_id,remove_comps_in_lkdn_table,max_comps_to_try)
        # running inside view search for each company and saving the output to table
        self.company_search_insideview_multi(comp_input_dets,list_id)
        # get all company ids which need to be searched for contacts from table
        comp_ids = self.get_companies_for_contact_search(list_id,comp_contries_loc)
        # if get_comp_dets_sep is True, search for each comp_id in insideview and save the company details
        if get_comp_dets_sep:
            self.get_save_company_details_from_insideview_listinput(list_id,comp_ids)
        # todo : we can add option to search companies using all the data fetched here
        # search for people from these comp_ids
        comp_id_str = ','.join([str(i) for i in comp_ids])
        filters_dic['companyIdsToInclude'] = comp_id_str
        contacts_list = self.search_contacts(**filters_dic)
        self.save_contacts_seach_res(list_id,contacts_list)
        # if no need to get contacts, return, else continue
        if get_contacts:
            self.fetch_people_details_from_company_ids_crawler_process(list_id,comp_ids,retrieve_comp_dets=not get_comp_dets_sep,
                                                       max_res_per_company=max_res_per_company,desig_list=desig_list)
        self.con.close_cursor()
        self.con.close_connection()

    def fetch_people_details_from_company_ids_crawler_process(self,list_id,comp_ids,max_res_per_company=3,
                                              retrieve_comp_dets=0,desig_list=[]),n_threads=10:
        '''
        :param list_id:
        :param comp_ids:
        :param max_res_per_company: max number of email searches to be done for each company
        :param filters_dic:
        :return:
        '''
        # get contact ids which are not present in the contacts table
        # todo: add the designations condition here to further filter. can be done by adding where condition on insideview_contact_search_res table
        if desig_list:
            desig_list_reg = '\y' + '\y|\y'.join(desig_list) + '\y'
            query = "SELECT distinct new_contact_id FROM " \
                    " (SELECT company_id,new_contact_id, ROW_NUMBER() OVER (PARTITION BY company_id ) AS Row_ID FROM " \
                    " ( select a.company_id,a.new_contact_id from " \
                    " crawler.insideview_contact_search_res a left join crawler.insideview_contact_data b " \
                    " on a.email_md5_hash=b.email_md5_hash where a.list_id = %s and a.company_id in %s and " \
                    " b.email_md5_hash is null and array_to_string(a.titles,',') ~* '{}' )x "\
                    "  ) as A " \
                    " WHERE Row_ID <= {} ".format(desig_list_reg,max_res_per_company)
        else:
            query = "SELECT distinct new_contact_id FROM " \
                    " (SELECT company_id,new_contact_id, ROW_NUMBER() OVER (PARTITION BY company_id ) AS Row_ID FROM " \
                    " ( select a.company_id,a.new_contact_id from " \
                    " crawler.insideview_contact_search_res a left join crawler.insideview_contact_data b " \
                    " on a.email_md5_hash=b.email_md5_hash where a.list_id = %s and a.company_id in %s and " \
                    " b.email_md5_hash is null )x"\
                    "  ) as A " \
                    " WHERE Row_ID <= {} ".format(max_res_per_company)
        self.con.cursor.execute(query,(list_id,tuple(comp_ids),))
        new_contact_ids = self.con.cursor.fetchall()
        new_contact_ids = [i[0] for i in new_contact_ids]
        def worker():
            while not self.in_queue.empty():
                contact_id = self.queue.get()
                res_dic = self.get_contact_info_from_id(contact_id,retrieve_comp_dets)
                self.out_queue.put(res_dic)
                self.in_queue.task_done()
        self.in_queue = Queue(maxsize=0)
        self.out_queue = Queue(maxsize=0)
        for i in range(n_threads):
            worker = threading.Thread(target=worker)
            worker.setDaemon(True)
            worker.start()
        for contact_id in new_contact_ids:
            self.in_queue.put(contact_id)
        time.sleep(120)
        while not self.out_queue.empty() or not self.in_queue.empty():
            res_dic = self.out_queue.get()
            self.save_contact_info(res_dic)
            self.out_queue.task_done()
        self.in_queue = None
        self.out_queue = None
        
    def company_search_insideview_multi(self,comp_input_dets,list_id,n_threads=10):
        '''search in insideview and save the results from the input list of companies
        '''
        def worker():
            while not self.in_queue.empty():
                comp_website,comp_name,list_items_id = self.queue.get()
                if comp_website:
                    comp_search_results = self.search_company_single(None,comp_website,max_no_results=10)
                else:
                    comp_search_results = self.search_company_single(comp_name,comp_website,max_no_results=10)
                self.out_queue.put((list_items_id,comp_search_results))
                self.in_queue.task_done()
        self.in_queue = Queue(maxsize=0)
        self.out_queue = Queue(maxsize=0)
        for i in range(n_threads):
            worker = threading.Thread(target=worker)
            worker.setDaemon(True)
            worker.start()
        for comp_website,comp_name,list_items_id in comp_input_dets:
            self.in_queue.put((comp_website,comp_name,list_items_id))
        time.sleep(120)
        while not self.out_queue.empty() or not self.in_queue.empty():
            list_items_id,comp_search_results = self.out_queue.get()
            if comp_search_results:
                self.save_company_search_res_single(list_id,list_items_id,comp_search_results)
            self.out_queue.task_done()
        self.in_queue = None
        self.out_queue = None
                
    def get_companies_for_contact_search(self,list_id,comp_contries_loc):
        ''' get the list of companies for which the insideview contact search will be done '''
        # todo: fix this logic
        if find_new_contacts_only:
            query = "select distinct a.company_id from crawler.insideview_company_search_res a left join " \
                    " crawler.insideview_contact_search_res b on a.list_id=b.list_id and a.company_id=b.company_id " \
                    " left join crawler.insideview_contact_data c on a.company_id=c.company_id" \
                    " where c.company_id is null and a.list_id=%s"
        else:
            query = "select distinct company_id from crawler.insideview_company_search_res a " \
                    " where list_id=%s "
        # if country based filters available, apply them
        if comp_contries_loc:
            countries = self.get_contries_list(comp_contries_loc)
            query = query + ' and a.country in %s'
            self.con.execute(query,(list_id,tuple(countries),))
        else:
            self.con.execute(query,(list_id,))
        comp_ids = self.con.cursor.fetchall()
        comp_ids = [i[0] for i in comp_ids]
        return comp_ids

    def get_save_company_details_from_insideview_listinput(self,list_id,comp_ids,n_threads=10):
        '''get company details from insideview and save the result for each company id in the list '''
        # find all company ids not present in the company_details table
        # todo: can add a timestamp related filter here later
        query = " select distinct a.company_id from crawler.insideview_company_search_res a " \
                    " left join crawler.insideview_company_details_contact_search b on a.company_id=b.company_id " \
                    " where a.list_id=%s and a.company_id in %s and b.company_id is null"
        self.con.execute(query,(list_id,tuple(comp_ids),))
        comp_ids_already_present = self.con.cursor.fetchall()
        comp_ids_already_present = [i[0] for i in comp_ids_already_present]
        comp_ids_not_preset = list(set(comp_ids)-set(comp_ids_already_present))
        def worker():
            while not self.in_queue.empty():
                comp_id = self.queue.get()
                comp_dets_dic = self.get_company_details_from_id(comp_id)
                self.out_queue.put(comp_dets_dic)
                self.in_queue.task_done()
        self.in_queue = Queue(maxsize=0)
        self.out_queue = Queue(maxsize=0)
        for i in range(n_threads):
            worker = threading.Thread(target=worker)
            worker.setDaemon(True)
            worker.start()
        for comp_id in comp_ids_not_preset:
            self.in_queue.put(comp_id)
        time.sleep(120)
        while not self.out_queue.empty() or not self.in_queue.empty():
            comp_dets_dic = self.out_queue.get()
            self.save_company_dets_dic_input(comp_dets_dic)
            self.out_queue.task_done()
        self.in_queue = None
        self.out_queue = None
    
    def get_dets_for_insideview_fetch(self,list_id,remove_comps_in_lkdn_table=0,max_comps_to_try=0):
        '''find the company names and urls which needs to be fetched from builtwith
        '''
        # todo: we can add a flag in list_items/list_items_urls table and use it here to avoid duplication
        # find the companies for whom the search was not done in builtwith
        query = " select distinct list_input,list_input_additional,a.id from crawler.list_items a left join " \
                " crawler.insideview_company_search_res b on a.list_id=b.list_id and a.id=b.list_items_id " \
                " where a.list_id = %s and b.list_id is null"
        self.con.execute(query,(list_id,))
        comp_input_dets_no_iv = self.con.cursor.fetchall()
        # find all company details not present in linkedin_company_base if remove_comps_in_lkdn_table flag is True
        if remove_comps_in_lkdn_table:
            query = "select distinct list_input,list_input_additional,a.id from crawler.list_items a  left join " \
                    " crawler.list_items_urls b on a.id=b.list_items_id and a.list_id=b.list_id left join crawler.linkedin_company_base c " \
                    " on b.id=c.list_items_url_id and a.list_id=c.list_id where a.list_id = %s and (b.url is null or c.linkedin_url is null)"
            self.con.execute(query,(list_id,))
            comp_input_dets_no_lkdn = self.con.cursor.fetchall()
        else:
            comp_input_dets_no_lkdn = comp_input_dets_no_iv #this is done coz of the intersection below
        # find intersection
        comp_input_dets = list(set(comp_input_dets_no_lkdn).intersection(set(comp_input_dets_no_iv)))
        shuffle(comp_input_dets)
        # try for max_comps_to_try at a time if max_comps_to_try present
        if max_comps_to_try:
            comp_input_dets = comp_input_dets[:min(max_comps_to_try,len(comp_input_dets))]
        return comp_input_dets

    def gen_filters_dic(self,filters_loc):
        '''saved as key,value in each row. this will be passed to the api filter search
        :param filters_loc: location of file
        :return:
        '''
        filter_dic = {}
        rows = []
        with open(filters_loc,'r') as f:
            reader = csv.reader(f)
            for row in reader:
                rows.append(row)
        for row in rows:
            key,value = row
            try:
                value_tmp = eval(value)
                if not type(value_tmp) == tuple:
                    value = value_tmp
            except:
                pass
            filter_dic[key] = value
        return filter_dic

    def get_contries_list(self,comp_contries_loc):
        '''
        :param comp_contries_loc:location of csv with country names
        :return:
        '''
        df = pd.read_csv(comp_contries_loc)
        return list(df['countries'])

    def search_company_single(self,name,website,ticker=False,max_no_results=99,results_per_page=50):
        '''
        :param name: company name for search
        :param website: website for search
        :param ticker: ticker
        :param max_no_results: save this many matching results for a company
        :param results_per_page:
        :return:
        '''
        out_list = []
        search_dic = {}
        if name:
            search_dic['name'] = name
        if website:
            search_dic['website'] = website
        if ticker:
            search_dic['ticker'] = ticker
        if not search_dic:
            raise ValueError('No searchable info about the company is present')
        search_dic['url'] = company_search_url
        search_dic['resultsPerPage'] = results_per_page
        # go through results till all search results are obtained. give maximum as 500 for safety
        total_results,res_page_no = 9999999,0
        while min(max_no_results,total_results) > res_page_no*results_per_page:
            res_page_no += 1
            search_dic['page'] = res_page_no
            r = requests.get(throttler_app_address,params=search_dic)
            res_dic = json.loads(r.text)
            if not res_dic.get('companies',None):
                break
            out_list.extend(res_dic.get('companies'))
            total_results = res_dic['totalResults']
        return out_list

    def save_company_search_res_single(self,list_id,list_items_id,res_list):
        '''
        :param list_id:
        :param list_items_id:
        :param res_list:
        :return:
        '''
        res_to_insert = [
            (list_id,list_items_id,res.get('city',None),res.get('state',None),res.get('country',None),
                res.get('name',None),res.get('companyId',None))
            for res in res_list
        ]
        records_list_template = ','.join(['%s'] * len(res_to_insert))
        insert_query = 'insert into crawler.insideview_company_search_res ' \
                       ' (list_id, list_items_id,city,state,country,name,company_id) values {}'.format(records_list_template)
        self.con.execute(insert_query, res_to_insert)
        self.con.commit()

    def search_contacts(self,max_no_results=9999,results_per_page=500,**kwargs):
        '''
        :param max_no_results: max no results needed
        :param results_per_page: how many result per page, max 500
        :param kwargs: filter options(all available options are given in the api ref page:
                    (https://kb.insideview.com/hc/en-us/articles/204395607--POST-Contact-List)
        :return:
        '''
        out_list = []
        search_dic = {'url':contact_search_url}
        kwargs['resultsPerPage'] = results_per_page
        total_results,res_page_no = 9999999,0
        while min(max_no_results,total_results) > res_page_no*results_per_page:
            res_page_no += 1
            kwargs['page'] = res_page_no
            r = requests.post(throttler_app_address,params=search_dic,json=kwargs)
            res_dic = json.loads(r.text)
            if not res_dic.get('contacts',None):
                break
            out_list.extend(res_dic.get('contacts'))
            total_results = int(res_dic['totalResults'])
        return out_list

    def save_contacts_seach_res(self,list_id,res_list):
        '''
        :param list_id:
        :param res_list:
        :return:
        '''
        # res_to_insert = [
        #     (list_id,res['firstName'],res['lastName'],res['fullName'],res['id'],res['peopleId'],res['titles'],
        #      res['active'],res['companyId'],res['companyName'],res['hasEmail'],res['emailMd5Hash'],res['hasPhone'],
        #      res['city'],res['state'],res['country'])
        #     for res in res_list
        # ]
        if res_list:
            res_to_insert = [
                (list_id,res.get('firstName',None),res.get('lastName',None),res.get('fullName',None),res.get('id',None),
                    res.get('peopleId',None),res.get('titles',None),res.get('active',None),res.get('companyId',None),
                    res.get('companyName',None),res.get('hasEmail',None),res.get('emailMd5Hash',None),
                    res.get('hasPhone',None),res.get('city',None),res.get('state',None),res.get('country',None))
                for res in res_list
            ]
            records_list_template = ','.join(['%s'] * len(res_to_insert))
            insert_query = 'insert into crawler.insideview_contact_search_res ' \
                           ' (list_id, first_name,last_name,full_name,new_contact_id,people_id,titles,active,company_id,' \
                           'company_name,has_email,email_md5_hash,has_phone,city,state,country) values' \
                           ' {} ON CONFLICT DO NOTHING'.format(records_list_template)
            self.con.execute(insert_query, res_to_insert)
            self.con.commit()

    def get_contact_info_from_id(self,new_contact_id,retrieve_comp_dets=1):
        '''
        :param new_contact_id:
        :param retrieve_comp_dets:
        :return:
        '''
        contact_url = contact_details_url.format(newcontactId=new_contact_id)
        search_dic = {'url':contact_url}
        if retrieve_comp_dets:
            search_dic['retrieveCompanyDetails'] = True
        r = requests.get(throttler_app_address,params=search_dic)
        res_dic = json.loads(r.text)
        return res_dic

    def save_contact_info(self,res_dic):
        '''
        :param list_id:
        :param res_dic:
        :return:
        '''
        # generating md5 hash of email
        md5_hasher = hashlib.md5()
        if res_dic.get('email',None):
            md5_hasher.update(res_dic.get('email',None))
            email_md5_hash = md5_hasher.hexdigest()
        else:
            email_md5_hash = None
        insert_column_template = ','.join(['%s']*23) #23 columns to insert
        insert_query = "insert into crawler.insideview_contact_data " \
                       " (contact_id,people_id,active,first_name,last_name,full_name,titles,company_id, " \
                       " company_name,age,description,email,email_md5_hash,job_function,job_levels,phone,salary, " \
                       " salary_currency,image_url,facebook_url,linkedin_url,twitter_url,sources) values " \
                       " ( {} ) ".format(insert_column_template)
        res_to_insert = (res_dic.get('contactId',None),res_dic.get('peopleId',None),res_dic.get('active',None),
                         res_dic.get('firstName',None),res_dic.get('lastName',None),res_dic.get('fullName',None),
                         res_dic.get('titles',None),res_dic.get('companyId',None),res_dic.get('companyName',None),
                         res_dic.get('age',None),res_dic.get('description',None),res_dic.get('email',None),email_md5_hash,
                         res_dic.get('jobFunction',None),res_dic.get('jobLevels',None),
                         res_dic.get('phone',None),res_dic.get('salary',None),res_dic.get('salaryCurrency',None),
                         res_dic.get('imageUrl',None),res_dic.get('facebookHandle',None),
                         res_dic.get('linkedinHandle',None),res_dic.get('twitterHandle',None),
                         res_dic.get('sources',None),)
        self.con.execute(insert_query,res_to_insert)
        self.con.commit()
        self.save_contact_info_misc(contact_id=res_dic.get('contactId',None),people_id=res_dic.get('peopleId',None),
                                    education=res_dic.get('education',[]))
        if 'companyDetails' in res_dic:
            self.save_company_dets_dic_input(res_dic['companyDetails'])

    def save_contact_info_misc(self,contact_id,people_id,education=[]):
        ''' json columns returned are saved separately
        :param contact_id:
        :param people_id:
        :param education:
        :return:
        '''
        # inserting british sics data
        education_query = 'insert into crawler.insideview_contact_education ' \
                             ' (contact_id,people_id,degree,major,university) values ' \
                             ' (%s,%s,%s,%s,%s) '
        for det_dic in education:
            if det_dic:
                degree,major,university = det_dic.get('degree',None),det_dic.get('major',None),det_dic.get('university',None)
                self.con.execute(education_query,(contact_id,people_id,degree,major,university,))

        self.con.commit()

    def save_company_dets_dic_input(self,res_dic):
        '''
        :param list_id:
        :param res_dic:
        :return:
        '''
        insert_column_template = ','.join(['%s']*46) #46 columns to insert
        insert_query = "insert into crawler.insideview_company_details_contact_search " \
                       " (company_id,company_status,company_type,name,websites,subsidiary,parent_company_id," \
                       "parent_company_name,parent_company_country,industry,industry_code,sub_industry," \
                       "sub_industry_code,sic,sic_description,naics,naics_description,employees,employee_range," \
                       "fortune_ranking,foundation_date,gender,ethnicity,dbe,wbe,mbe,vbe,disabled,lgbt,revenue," \
                       "revenue_currency,revenue_range,most_recent_quarter,financial_year_end,phone,fax,street,city,state,country," \
                       "zip,equifax_id,ultimate_parent_company_id,ultimate_parent_company_name," \
                       "ultimate_parent_company_country,sources) values " \
                       " ( {} ) ".format(insert_column_template)
        res_to_insert = (res_dic.get('companyId',None),res_dic.get('companyStatus',None),
                         res_dic.get('companyType',None),res_dic.get('name',None),res_dic.get('websites',None),
                         res_dic.get('subsidiary',None),res_dic.get('parentCompanyId',None),
                         res_dic.get('parentCompanyName',None),res_dic.get('parentCompanyCountry',None),
                         res_dic.get('industry',None),res_dic.get('industryCode',None),res_dic.get('subIndustry',None),
                         res_dic.get('subIndustryCode',None),res_dic.get('sic',None),res_dic.get('sicDescription',None),
                         res_dic.get('naics',None),res_dic.get('naicsDescription',None),
                         res_dic.get('employees',None),res_dic.get('employeeRange',None),res_dic.get('fortuneRanking',None),
                         res_dic.get('foundationDate',None),res_dic.get('gender',None),res_dic.get('ethnicity',None),
                         res_dic.get('dbe',None),res_dic.get('wbe',None),res_dic.get('mbe',None),
                         res_dic.get('vbe',None),res_dic.get('disabled',None),res_dic.get('lgbt',None),
                         res_dic.get('revenue',None),res_dic.get('revenueCurrency',None),res_dic.get('revenueRange',None),
                         res_dic.get('mostRecentQuarter',None),res_dic.get('financialYearEnd',None),
                         res_dic.get('phone',None),res_dic.get('fax',None),res_dic.get('street',None),
                         res_dic.get('city',None),res_dic.get('state',None),res_dic.get('country',None),
                         res_dic.get('zip',None),res_dic.get('equifaxId',None),
                         res_dic.get('ultimateParentId',None),res_dic.get('ultimateParentCompanyName',None),
                         res_dic.get('ultimateParentCompanyCountry',None),res_dic.get('sources',None),)
        self.con.execute(insert_query,res_to_insert)
        self.con.commit()
        # save ticker and britishSics separately
        self.save_company_dets_misc(res_dic.get('companyId',None),res_dic.get('britishSics',[]),
                                    res_dic.get('tickers',[]))

    def save_company_dets_misc(self,company_id,british_sics=[],tickers=[]):
        '''
        :param british_sics: dictionary
        :param tickers: dictionary
        :return:
        '''
        # inserting british sics data
        british_sics_query = 'insert into crawler.insideview_company_british_sics ' \
                             ' (company_id,british_sic,description) values ' \
                             ' (%s,%s,%s) '
        for det_dic in british_sics:
            sic_value,description = det_dic.get('britishSic',None),det_dic.get('description',None)
            self.con.execute(british_sics_query,(company_id,sic_value,description,))
        # inserting tickers data
        tickers_query = ' insert into crawler.insideview_company_tickers ' \
                        ' (company_id,ticker_name,exchange) values (%s,%s,%s)'
        for det_dic in tickers:
            ticker_name,exchange = det_dic.get('tickerName',None),det_dic.get('exchange',None)
            self.con.execute(tickers_query,(company_id,ticker_name,exchange,))
        self.con.commit()

    def get_company_details_from_id(self,company_id):
        '''
        :param company_id:
        :return:
        '''
        contact_url = company_details_url.format(companyId=company_id)
        search_dic = {'url':contact_url}
        r = requests.get(throttler_app_address,params=search_dic)
        res_dic = json.loads(r.text)
        return res_dic

    def get_company_tech_profile_from_id(self,company_id):
        '''
        :param company_id:
        :return:
        '''
        contact_url = company_tech_profile_url.format(companyId=company_id)
        search_dic = {'url':contact_url}
        r = requests.get(throttler_app_address,params=search_dic)
        res_dic = json.loads(r.text)
        # out_list = self.expand_tech_profile_dic(company_id,res_dic)
        return res_dic

    def expand_tech_profile_dic(self,company_id,res_dic):
        ''' techprofile dictionary is in nested format. expand it so that it can be saved to table
        :param company_id:
        :param res_dic:
        :return:
        '''
        out_list = []
        for category_dic in res_dic.get('categories',[]):
            category_name = category_dic.get('categoryName',None)
            category_id = category_dic.get('categoryId',None)
            for sub_category_dic in category_dic.get('subCategories',[]):
                sub_category_id = sub_category_dic.get('subCategoryId',None)
                sub_category_name = sub_category_dic.get('subCategoryName',None)
                for product_dic in sub_category_dic.get('products',[]):
                    product_name = product_dic['productName']
                    product_id = product_dic['productId']
                    out_list.append((company_id,category_id,category_name,sub_category_id,sub_category_name,
                                        product_id,product_name))
        return out_list

    def save_company_techs(self,company_id,res_dic):
        '''
        :param company_id:
        :param res_dic:this will be the output of  get_company_tech_profile_from_id function
        :return:
        '''
        res_list = self.expand_tech_profile_dic(company_id,res_dic)
        records_list_template = ','.join(['%s'] * len(res_list))
        insert_query = 'insert into crawler.insideview_company_tech_details ' \
                       ' (company_id,category_id,category_name,sub_category_id,sub_category_name,' \
                       ' product_id,product_name) values' \
                       ' {}'.format(records_list_template)
        self.con.execute(insert_query, res_list)
        self.con.commit()

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
    optparser.add_option('--get_contacts',
                         dest='get_contacts',
                         help='get contact details and emails if 1',
                         default=1,type='int')
    optparser.add_option('--max_res_per_company',
                         dest='max_res_per_company',
                         help='max number of email searches to be done for each company',
                         default=5,type='int')
    optparser.add_option('--get_comp_dets_sep',
                         dest='get_comp_dets_sep',
                         help='if 1,get company details also while searching for contacts(works only if get_contacts is 1)',
                         default=1,type='int')
    optparser.add_option('--remove_comps_in_lkdn_table',
                         dest='remove_comps_in_lkdn_table',
                         help="if 1, try to find contacts only for companies for whom we don't have data in linkedin company table",
                         default=0,type='int')
    optparser.add_option('--find_new_contacts_only',
                         dest='find_new_contacts_only',
                         help="if 1, try to find contacts for companies for whom we don't have enough contact details",
                         default=0,type='int')
    optparser.add_option('-d', '--designations',
                         dest='desig_loc',
                         help='location of csv containing target designations',
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
    find_new_contacts_only = options.find_new_contacts_only
    desig_loc = options.desig_loc

    if desig_loc:
        inp_df = pd.read_csv(desig_loc)
        desig_list = list(inp_df[designations_column_name])
    else:
        desig_list = None
    # checks
    if not list_name:
        raise ValueError('need list name to run')
    if not filter_loc:
        raise ValueError('need filter location to run')
    if not out_loc:
        raise ValueError('need output folder location')
    fetcher = InsideviewFetcher()
    fetcher.fetch_data_csv_input(list_name=list_name,inp_loc=inp_loc,out_loc=out_loc,filters_loc=filter_loc,
                                 max_comps_to_try=max_comps_to_try,get_contacts=get_contacts,
                                 get_comp_dets_sep=get_comp_dets_sep,max_res_per_company=max_res_per_company,
                                 remove_comps_in_lkdn_table=remove_comps_in_lkdn_table,
                                 find_new_contacts_only=find_new_contacts_only,desig_list=desig_list)
