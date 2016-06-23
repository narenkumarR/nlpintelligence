__author__ = 'joswin'

import postgres_connect

class TablesBackup(object):
    '''
    '''
    def __init__(self):
        self.con = postgres_connect.PostgresConnect()

    def clean_tables(self):
        '''
        :return:
        '''
        self.con.get_cursor()
        query = 'insert into linkedin_company_base_bck select * from linkedin_company_base'
        self.con.cursor.execute(query)
        query = 'insert into linkedin_people_base_bck select * from linkedin_people_base'
        self.con.cursor.execute(query)
        query = 'insert into linkedin_company_urls_to_crawl_bck select * from linkedin_company_urls_to_crawl on conflict do nothing'
        self.con.cursor.execute(query)
        query = 'insert into linkedin_people_urls_to_crawl_bck select * from linkedin_people_urls_to_crawl on conflict do nothing'
        self.con.cursor.execute(query)
        query = 'insert into linkedin_company_finished_urls_bck select * from linkedin_company_finished_urls on conflict do nothing'
        self.con.cursor.execute(query)
        query = 'insert into linkedin_people_finished_urls_bck select * from linkedin_people_finished_urls on conflict do nothing'
        self.con.cursor.execute(query)
        self.con.commit()
        self.con.close_cursor()
