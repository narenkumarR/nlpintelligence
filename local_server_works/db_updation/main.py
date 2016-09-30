__author__ = 'joswin'
import psycopg2

from constants import database,user,password,host

class Util(object):
    def __init__(self):
        '''
        :return:
        '''
        self.con = psycopg2.connect(database='pipecandy_db1', user='pipecandy_user',password='pipecandy',host='localhost')

    def create_tables(self):
        '''
        :return:
        '''
        self.cursor = self.con.cursor()
        query = 'DROP TABLE IF EXISTS linkedin_company_base_DATE'
        self.cursor.execute(query)
        query = 'DROP TABLE IF EXISTS linkedin_people_base_DATE'
        self.cursor.execute(query)
        self.con.commit()
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
        self.con.commit()
        self.cursor.close()

    def insert_to_table(self):
        '''
        :return:
        '''
