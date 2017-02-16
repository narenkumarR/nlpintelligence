__author__ = 'joswin'
# -*- coding: utf-8 -*-
import datetime

from postgres_connect import PostgresConnect
from constants import database,host,user,password

class API_counter(object):
    def __init__(self,list_id):
        self.list_id = list_id
        self.con = PostgresConnect(database_in=database,host_in=host,
                                                    user_in=user,password_in=password)
        self.get_list_api_counts()

    def get_list_api_counts(self):
        self.con.get_cursor()
        query = " select company_search_hits,company_details_hits,newcontact_search_hits,newcontact_email_hits" \
                ",people_search_hits,contact_fetch_hits,news_api_hits " \
                " from crawler.insideview_api_hits where list_id = %s"
        self.con.cursor.execute(query,(self.list_id,))
        tmp = self.con.cursor.fetchall()
        if not tmp:
            self.create_api_counts()
            self.get_list_api_counts()
        else:
            self.company_search_hits,self.company_details_hits,self.newcontact_search_hits,self.newcontact_email_hits,\
                        = tmp[0][0],tmp[0][1],tmp[0][2],tmp[0][3]
            self.people_search_hits,self.contact_fetch_hits,self.news_api_hits = tmp[0][4],tmp[0][5],tmp[0][5]
        self.con.close_cursor()

    def create_api_counts(self):
        ''' creating counts for first time'''
        query = " insert into crawler.insideview_api_hits " \
                " (list_id,company_search_hits,company_details_hits,newcontact_search_hits,newcontact_email_hits," \
                "people_search_hits,contact_fetch_hits,news_api_hits) " \
                " values (%s,%s,%s,%s,%s,%s,%s,%s)"
        self.con.cursor.execute(query,(self.list_id,0,0,0,0,0,0,0,))
        self.con.commit()

    def update_list_api_counts(self):
        ''' '''
        self.con.get_cursor()
        query = "update crawler.insideview_api_hits set company_search_hits=%s,company_details_hits=%s," \
                "newcontact_search_hits=%s,newcontact_email_hits=%s,people_search_hits=%s,contact_fetch_hits=%s," \
                "news_api_hits=%s, updated_on=%s " \
                "where list_id=%s "
        cur_time = datetime.datetime.now()
        self.con.cursor.execute(query,(self.company_search_hits,self.company_details_hits,self.newcontact_search_hits,
                                self.newcontact_email_hits,self.people_search_hits,self.contact_fetch_hits,
                                self.news_api_hits,cur_time,self.list_id))
        self.con.commit()
        self.con.close_cursor()
        self.con.close_connection()
