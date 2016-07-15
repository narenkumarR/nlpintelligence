__author__ = 'joswin'

import psycopg2
from constants import database,user,password,host

class PostgresConnect(object):
    '''
    '''
    def __init__(self,database_in=None,user_in=None,password_in=None,host_in=None):
        '''
        :return:
        '''
        self.connect(database_in,user_in,password_in,host_in)

    def connect(self,database_in=None,user_in=None,password_in=None,host_in=None):
        '''
        :return:
        '''
        if user_in and host_in and password_in:
            self.con = psycopg2.connect(database=database_in, user=user_in,password=password_in,host=host_in)
        else:
            self.con = psycopg2.connect(database=database, user=user,password=password,host=host)
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

    def execute(self,query,args=(),commit=False):
        '''
        :param query:
        :param commit : if commit, commit at the end
        :return:
        '''
        if args:
            self.cursor.execute(query,args)
        else:
            self.cursor.execute(query)
        if commit:
            self.commit()

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

