__author__ = 'joswin'

import psycopg2

class PostgresConnect(object):
    '''
    '''
    def __init__(self):
        '''
        :return:
        '''
        self.connect()

    def connect(self):
        '''
        :return:
        '''
        self.con = psycopg2.connect(database='linkedin_data', user='postgres',password='postgres',host='localhost')
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

