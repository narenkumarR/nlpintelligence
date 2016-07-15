__author__ = 'joswin'

from constants import prospect_database,prospect_host,prospect_user,prospect_password

import logging
import postgres_connect


class FetchProspectDB(object):
    '''
    '''
    def __init__(self):
        self.con = postgres_connect.PostgresConnect(database_in=prospect_database,host_in=prospect_host,
                                                    user_in=prospect_user,password_in=prospect_password)

    def fetch_data(self,list_id,desig_list=None,similar_companies=1):
        '''
        :param list_id:
        :param desig_list:
        :param similar_companies:
        :return:
        '''
        pass