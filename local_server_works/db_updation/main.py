__author__ = 'joswin'
import psycopg2

from constants import from_database,from_host,from_password,from_user,from_port,to_database,to_host,to_password,to_user,to_port

class Util(object):
    def __init__(self):
        '''
        :return:
        '''
        self.to_con = psycopg2.connect(database=to_database, user=to_user,password=to_password,host=to_host,port=to_port)
        self.from_con = psycopg2.connect(database=from_database, user=from_user,password=from_password,host=from_host,port=from_port)

    def create_tables(self):
        '''
        :return:
        '''
        self.cursor = self.to_con.cursor()
        query = 'DROP TABLE IF EXISTS linkedin_company_base_DATE'
        self.cursor.execute(query)
        query = 'DROP TABLE IF EXISTS linkedin_people_base_DATE'
        self.cursor.execute(query)
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
        self.cursor.execute(query)
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
        self.cursor.execute(query)
        self.to_con.commit()
        self.cursor.close()

    def insert_to_table(self):
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

        dump_command = 'psql --dbname=postgresql://{from_user}:{from_password}@{from_host}:{from_port}/{from_db} '\
                    '-c " copy (select * from crawler.linkedin_company_base) "'
        pass