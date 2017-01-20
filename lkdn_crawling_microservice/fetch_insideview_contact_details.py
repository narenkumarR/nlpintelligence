__author__ = 'joswin'
# -*- coding: utf-8 -*-

import pandas as pd
import json
import requests

from sqlalchemy import create_engine

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

    def main(self,list_name,out_loc,inp_loc=None,desig_loc=None):
        '''
        :param list_name:
        :param inp_loc:
        :param desig_loc:
        :return:
        '''
        list_id = self.create_list_id(list_name)
        if inp_loc:
            self.insert_input_to_db(inp_loc,list_id)
        self.search_contact_for_people(list_id)
        self.search_email_for_contact(list_id,desig_loc)
        self.save_results(list_id,list_name,out_loc)

    def save_results(self,list_id,list_name,out_loc):
        '''
        :param list_id:
        :return:
        '''
        engine = create_engine('postgresql://{user_name}:{password}@{host}:{port}/{database}'.format(
            user_name=user,password=password,host=host,port='5432',database=database
        ))
        query = " select input_filters,c.* from crawler.list_input_insideview_contacts a join " \
                " crawler.insideview_contact_name_search_res b on a.list_id=b.list_id and a.id=b.list_items_id join " \
                " crawler.insideview_contact_data c on b.contact_id = c.contact_id " \
                "where a.list_id = '{}' ".format(list_id)
        df = pd.read_sql_query(query,engine)
        df.to_csv('{}/{}_contacts_emails.csv'.format(out_loc,list_name),index=False,quoting=1,encoding='utf-8')
        query = " select input_filters,b.* from crawler.list_input_insideview_contacts a join " \
                " crawler.insideview_contact_name_search_res b on a.list_id=b.list_id and a.id=b.list_items_id  " \
                "where a.list_id = '{}' ".format(list_id)
        df = pd.read_sql_query(query,engine)
        df.to_csv('{}/{}_contacts_search_results.csv'.format(out_loc,list_name),index=False,quoting=1,encoding='utf-8')
        engine.dispose()

    def search_email_for_contact(self,list_id,desig_loc=None):
        '''
        :param list_id:
        :param desig_loc:
        :return:
        '''
        self.con.get_cursor()
        if desig_loc:
            inp_df = pd.read_csv(desig_loc)
            desig_list = list(inp_df[designations_column_name])
        else:
            desig_list = None
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
        for contact_id in contact_ids:
            res_dic = self.get_contact_details_from_contactid(contact_id)
            self.insideview_fetcher.save_contact_info(res_dic)
        self.con.commit()
        self.con.close_cursor()

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
        for res in list_inputs:
            list_input_id,search_dic = res[0],res[1]
            search_dic = json.loads(search_dic)
            search_dic['isEmailRequired'] = True
            search_results = self.search_insideview_contact(search_dic)
            if search_results:
                self.save_contact_search_res_single(list_id,list_input_id,search_results)
        self.con.commit()
        self.con.close_cursor()

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