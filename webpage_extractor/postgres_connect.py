__author__ = 'joswin'

import psycopg2
import logging
from constants import database,user,password,host

class PostgresConnect(object):
    '''
    '''
    def __init__(self,database_in=None,user_in=None,password_in=None,host_in=None):
        '''
        :return:
        '''
        if user_in and host_in and password_in and database_in:
            self.user,self.host,self.password,self.database = user_in,host_in,password_in,database_in
        else:
            self.user,self.host,self.password,self.database = user,host,password,database
        self.connect()

    def connect(self):
        '''
        :return:
        '''
        self.con = psycopg2.connect(database=self.database, user=self.user,password=self.password,host=self.host)
        self.cursor = self.con.cursor()
        # self.get_cursor()

    def get_cursor(self):
        '''
        :return:
        '''
        if not self.con.closed:
            self.connect()
        self.cursor = self.con.cursor()

    def close_cursor(self):
        '''
        :return:
        '''
        self.cursor.close()

    def close_connection(self):
        '''
        :return:
        '''
        self.con.close()

    def execute(self,query,args=(),commit=False):
        '''
        :param query:
        :param commit : if commit, commit at the end
        :return:
        '''
        try:
            if self.cursor.closed:
                self.get_cursor()
            if args:
                self.cursor.execute(query,args)
            else:
                self.cursor.execute(query)
            if commit:
                self.commit()
        except:
            logging.exception('error happened while trying to execute query:{}, args:{}'.format(query,args))
            self.con.rollback()
            # self.close_cursor()
            # self.get_cursor()

    def execute_async(self,query,commit = False):
        ''' for parallel processes, check if connection is executing something. execute only if returns false
        :param query:
        :param commit: if commit, commit at the end
        :return:
        '''
        while True:
            if not self.con.isexecuting():
                self.cursor.execute(query)
        if commit:
            self.commit()

    def commit(self):
        self.con.commit()

