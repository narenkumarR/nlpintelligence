__author__ = 'joswin'
import datetime
import psycopg2
import pickle
import os

import logging

from constants import from_database,from_host,from_password,from_user,from_port,to_database,to_host,to_password,\
    to_user,to_port,prev_run_time_loc
from table_updation_new import LinkedinDumpUtil

class CrawlerToServerDumper(object):
    def __init__(self):
        '''
        :return:
        '''
        pass

    def create_tables(self):
        '''
        :return:
        '''
        cursor = self.to_con.cursor()
        query = 'DROP TABLE IF EXISTS linkedin_company_base_DATE'
        cursor.execute(query)
        query = 'DROP TABLE IF EXISTS linkedin_people_base_DATE'
        cursor.execute(query)
        self.to_con.commit()
        query = "CREATE TABLE linkedin_company_base_DATE ("\
                "id uuid DEFAULT uuid_generate_v1mc() NOT NULL,"\
                "linkedin_url text,"\
                "company_name text,"\
                "company_size text,"\
                "industry text,"\
                "company_type text,"\
                "headquarters text,"\
                "description text,"\
                "founded text,"\
                "specialties text,"\
                "website text,"\
                "employee_details text,"\
                "also_viewed_companies text,"\
                "list_id uuid,"\
                "list_items_url_id uuid,"\
                "created_on timestamp without time zone DEFAULT now()"\
                ") "
        cursor.execute(query)
        query = "CREATE TABLE linkedin_people_base_DATE ("\
                "id uuid DEFAULT uuid_generate_v1mc() NOT NULL,"\
                "linkedin_url text,"\
                "name text,"\
                "sub_text text,"\
                "location text,"\
                "company_name text,"\
                "company_linkedin_url text,"\
                "previous_companies text,"\
                "education text,"\
                "industry text,"\
                "summary text,"\
                "skills text,"\
                "experience text,"\
                "related_people text,"\
                "same_name_people text,"\
                "list_id uuid,"\
                "list_items_url_id uuid,"\
                "created_on timestamp without time zone DEFAULT now()"\
                ") "
        cursor.execute(query)
        self.to_con.commit()
        cursor.close()

    def insert_to_tmp_tables(self):
        '''
        :return:
        '''
        # psql --dbname=postgresql://postgres:postgres@localhost:5432/crawler_service_test -c
        # "copy (select * from crawler.linkedin_company_base where created_on> '2016-09-08 14:00:00'  ) to stdout" |
        # psql --dbname=postgresql://pipecandy_user:pipecandy@192.168.1.142:5432/pipecandy_db1
        #  -c "copy linkedin_company_base_DATE from stdin"

        # psql --dbname=postgresql://postgres:postgres@localhost:5432/crawler_service_test -c
        #  "copy (select * from crawler.linkedin_people_base where created_on> '2016-09-08 14:00:00' ) to stdout" |
        # psql --dbname=postgresql://pipecandy_user:pipecandy@192.168.1.142:5432/pipecandy_db1
        #  -c "copy linkedin_people_base_DATE from stdin"
        logging.info('dumping company table to tmp table in server')
        comp_dump_command = '''psql --dbname=postgresql://{from_user}:{from_password}@{from_host}:{from_port}/{from_db} '''\
                    '''-c " copy (select * from crawler.linkedin_company_base where  created_on > '{from_time}' ''' \
                    ''' and created_on <= '{to_time}' ) to stdout " | '''\
                    ''' psql --dbname=postgresql://{to_user}:{to_password}@{to_host}:{to_port}/{to_db} -c '''\
                    ''' " copy linkedin_company_base_DATE from stdin " '''.format(from_user=from_user,
                                            from_password=from_password,from_host=from_host,from_port=from_port,
                                            from_db=from_database,from_time = self.prev_run_time,to_time = self.cur_run_time,
                                            to_user=to_user,to_password=to_password,to_host=to_host,to_port=to_port,
                                            to_db = to_database)
        os.system(comp_dump_command)
        logging.info('dumping people table to tmp table in server')
        ppl_dump_command = '''psql --dbname=postgresql://{from_user}:{from_password}@{from_host}:{from_port}/{from_db} '''\
                    '''-c " copy (select * from crawler.linkedin_people_base where  created_on > '{from_time}' ''' \
                    ''' and created_on <= '{to_time}' ) to stdout " | '''\
                    ''' psql --dbname=postgresql://{to_user}:{to_password}@{to_host}:{to_port}/{to_db} -c '''\
                    ''' " copy linkedin_people_base_DATE from stdin " '''.format(from_user=from_user,
                                            from_password=from_password,from_host=from_host,from_port=from_port,
                                            from_db=from_database,from_time = self.prev_run_time,to_time = self.cur_run_time,
                                            to_user=to_user,to_password=to_password,to_host=to_host,to_port=to_port,
                                            to_db = to_database)

        os.system(ppl_dump_command)

    def insert_to_main_tables(self,company_table= 'linkedin_company_base_DATE',
                              people_table = 'linkedin_people_base_DATE', array_present=False,
                              timestamp_col = 'created_on'):
        '''
        :param company_table:
        :param people_table:
        :param array_present:
        :param timestamp_col:
        :return:
        '''
        dump_util = LinkedinDumpUtil()
        if people_table:
            dump_util.process_people_dump(people_table,array_present=array_present,timestamp_col=timestamp_col)
        if company_table:
            dump_util.process_company_dump(company_table,array_present=array_present,timestamp_col=timestamp_col)
            dump_util.update_company_mapper(company_table)
        dump_util.con.close()

    def dump_main(self):
        '''
        :return:
        '''
        logging.basicConfig(filename='log_file.log', level=logging.INFO,format='%(asctime)s %(message)s')
        with open(prev_run_time_loc,'r') as f:
            self.prev_run_time = pickle.load(f)
        logging.info('previous run time : {}'.format(self.prev_run_time))
        self.cur_run_time = str(datetime.datetime.now())
        self.to_con = psycopg2.connect(database=to_database, user=to_user,password=to_password,host=to_host,port=to_port)
        logging.info('creating tmp table in server')
        self.create_tables()
        self.to_con.close()
        logging.info('inserting to tmp table in server from crawler table')
        self.insert_to_tmp_tables()
        logging.info('inserting to mail table in server from tmp table')
        self.insert_to_main_tables()
        with open(prev_run_time_loc,'w') as f:
            pickle.dump(self.cur_run_time,f)
        logging.info('completed process. New prev run time : {}'.format(self.cur_run_time))

if __name__ == "__main__":
    dumper = CrawlerToServerDumper()
    dumper.dump_main()
