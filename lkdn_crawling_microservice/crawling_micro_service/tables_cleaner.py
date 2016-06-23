__author__ = 'joswin'

import postgres_connect

class TablesCleaner(object):
    '''
    '''
    def __init__(self):
        self.con = postgres_connect.PostgresConnect()

    def clean_tables(self):
        '''
        :return:
        '''
        self.con.get_cursor()
        query = 'delete from linkedin_company_base'
        self.con.cursor.execute(query)
        query = 'delete from linkedin_people_base'
        self.con.cursor.execute(query)
        query = 'delete from linkedin_company_urls_to_crawl'
        self.con.cursor.execute(query)
        query = 'delete from linkedin_people_urls_to_crawl'
        self.con.cursor.execute(query)
        query = 'delete from linkedin_company_urls_to_crawl_priority'
        self.con.cursor.execute(query)
        query = 'delete from linkedin_people_urls_to_crawl_priority'
        self.con.cursor.execute(query)
        query = 'delete from linkedin_company_finished_urls'
        self.con.cursor.execute(query)
        query = 'delete from linkedin_people_finished_urls'
        self.con.cursor.execute(query)
        query = 'delete from linkedin_company_urls_to_crawl_initial_list'
        self.con.cursor.execute(query)
        query = 'delete from linkedin_people_designations'
        self.con.cursor.execute(query)
        self.con.commit()
        self.con.close_cursor()