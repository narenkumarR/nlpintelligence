__author__ = 'joswin'
# -*- coding: utf-8 -*-

import pandas as pd
import sqlalchemy

class DataReader(object):
    '''
    '''
    def __init__(self):
        pass

    def read_csv(self,loc):
        '''
        :param loc:
        :return:
        '''
        df = pd.read_csv(loc)
        return df
    
    def read_excel(self,loc):
        '''
        :param loc:
        :return:
        '''
        df = pd.read_excel(loc)
        return df

    def get_query(self,query,database,user,password,host,port=5432,con_string=''):
        '''
        :param query:query
        :param database:
        :param user:
        :param password:
        :param host:
        :param port:
        :param con_string:
        :return:
        '''
        if not con_string:
            # postgresql://scott:tiger@localhost/test
            con_string = 'postgresql://{user}:{password}@{host}:{port}/{database}'.format(user=user,password=password,
                                                                                host=host,port=port,database=database)
        engine = sqlalchemy.create_engine(con_string)
        df = pd.read_sql_query(query)
        return df