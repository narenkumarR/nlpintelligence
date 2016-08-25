__author__ = 'joswin'

from constants import prospect_database,prospect_host,prospect_user,prospect_password
from constants import database,host,user,password

import logging
import postgres_connect

from constants import desig_list_regex

def chunker(seq, size):
    return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))


class FetchProspectDB(object):
    '''
    '''
    def __init__(self):
        self.prospect_con = postgres_connect.PostgresConnect(database_in=prospect_database,host_in=prospect_host,
                                                    user_in=prospect_user,password_in=prospect_password)
        self.con = postgres_connect.PostgresConnect(database_in=database,host_in=host,
                                                    user_in=user,password_in=password)

    def fetch_data(self,list_id,prospect_query='',similar_companies=0,desig_list=[]):
        '''this method will first look at all the linkedin urls in the list_items_urls table in the prospect db.
        if they are present, fetch their data. if similar companies is 1, fetch for the similar companies also.
        The prospect query field can be used to query the prospect db. Here conditions can be given.
        Note: while changing the queries, it would be easier if we select the columns in the same order as insert
        :param list_id:
        :param prospect_query: all conditions should start with 'a.' (eg: a.location ~* 'united states' and a.other conditions)
        :param desig_list:
        :param similar_companies:
        :return:
        '''
        logging.info('fetch_prospects: Started fetching')
        if not desig_list:
            desig_list_reg = desig_list_regex
        else:
            desig_list_reg = '\y' + '\y|\y'.join(desig_list) + '\y'
        # first get all urls in the list_items_urls table
        self.con.get_cursor()
        self.prospect_con.get_cursor()
        query = "select distinct url,a.id from crawler.list_items_urls a left join crawler.linkedin_company_base b "\
                " on a.list_id = b.list_id and a.id = b.list_items_url_id where a.list_id = %s and b.list_id is null"
        self.con.execute(query,(list_id,))
        urls = self.con.cursor.fetchall()
        if urls:
            urls_dict = {}
            for url,list_items_url_id in urls:
                urls_dict[url] = list_items_url_id
            # put data from prospect db to crawler db
            logging.info('fetch_prospects: fetching company details based on linkedin urls')
            self.prospect_insert_company(urls,urls_dict,list_id,desig_list_reg=desig_list_reg)
            logging.info('fetch_prospects: fetching people details based on linkedin_urls')
            self.prospect_insert_people(urls,urls_dict,list_id,desig_list_reg=desig_list_reg)
        # take domains in list_table and search for them in prospect table
        # Here find all urls for which there is no url in list_items_urls table and give it to a function
        # function will find matches from prospect db, and update list_items_urls table and base tables
        query = "select regexp_replace(regexp_replace(a.list_input,'http://|https://',''),'www\.','') as domain,a.id "\
                "from crawler.list_items a left join crawler.list_items_urls b on a.list_id=b.list_id and a.id=b.list_items_id "\
                "where a.list_id = %s and a.list_input like '%%.co%%' and b.id is null"
        self.con.execute(query,(list_id,))
        domains = self.con.cursor.fetchall()
        if domains:
            domains_dict = {}
            for domain,list_items_id in domains:
                if domain[-1] == '/':
                    domain = domain[:-1]
                domains_dict[domain] = list_items_id
            logging.info('fetch_prospects: fetching details based on company domains')
            self.prospect_insert_company_website_input(domains,domains_dict,list_id,desig_list_reg=desig_list_reg)
        # querying using prospect query
        if prospect_query:
            logging.info('fetch_prospects: fetching based on prospect query')
            # first insert a default value into list_items table and list_items_urls table
            query = "insert into crawler.list_items (list_id,list_input,list_input_additional) "\
                                        " values (%s,%s,%s) on conflict do nothing "
            self.con.cursor.execute(query,(list_id,'data_from_prospect_table','prospect_db_data',))
            self.con.commit()
            query = "select id from crawler.list_items where list_id = %s and list_input = 'data_from_prospect_table' "\
                    "and list_input_additional = 'prospect_db_data' "
            self.con.cursor.execute(query,(list_id,))
            tmp = self.con.cursor.fetchall()
            list_item_id = tmp[0][0]
            self.con.cursor.execute("insert into crawler.list_items_urls (list_id,list_items_id,url) "\
                                        " values (%s,%s,%s) on conflict do nothing ",(list_id,list_item_id,'no_url_default',))
            self.con.commit()
            query = "select id from crawler.list_items_urls where list_id = %s and list_items_id = %s  and url = %s"
            self.con.cursor.execute(query,(list_id,list_item_id,'no_url_default',))
            tmp = self.con.cursor.fetchall()
            list_items_url_id = tmp[0][0]
            self.prospect_insert_from_query(prospect_query,list_id,list_items_url_id,desig_list_reg=desig_list_reg)
            logging.info('fetch_prospects: completed fetching based on prospect query')
        self.fix_redirect_urls(list_id) #fetching from db causes redirect url issues
        self.con.close_cursor()
        self.prospect_con.close_cursor()
        self.con.close_connection()
        self.prospect_con.close_connection()
        logging.info('fetch_prospects: completed fetching process')

    def prospect_insert_company(self,urls_all,urls_dict,list_id,desig_list_reg):
        '''search for urls in company table
        :param urls_all: list of tuples. first element should be the url
        :param urls_dict: key: url, value: list_items_url_id
        :param list_id:
        :return:
        '''
        for urls in chunker(urls_all,100):
            query = "select linkedin_url,company_name,company_size,industry,company_type,headquarters,description,"\
                    "founded,specialties,website,array_to_string(employee_details_array,'|') employee_details,"\
                    "array_to_string(also_viewed_companies_array,'|') also_viewed_companies "\
                    "from linkedin_company_base where linkedin_url in %s"
            self.prospect_con.execute(query,(tuple([i[0] for i in urls]),))
            company_prospect_data = self.prospect_con.cursor.fetchall()
            logging.info('fetch_prospects: company linkedin url - fetched companies : {}'.format(len(company_prospect_data)))
            if not company_prospect_data:
                continue
            # insert into crawler base table
            records_list_template = ','.join(['%s']*len(company_prospect_data))
            query = "insert into crawler.linkedin_company_base "\
                "(linkedin_url,company_name,company_size,industry,company_type,headquarters,description,founded,"\
                    "specialties,website,employee_details,also_viewed_companies,list_id,list_items_url_id) "\
                " VALUES {} ON CONFLICT DO NOTHING ".format(records_list_template)
            insert_list = [list(in_tup) for in_tup in company_prospect_data]
            insert_list1 = [tuple(i+[list_id,urls_dict[i[0]]]) for i in insert_list]
            self.con.cursor.execute(query, insert_list1)
            self.con.commit()
            # insert people for these companies into the people table
            query = "select d.linkedin_url,d.name,d.sub_text,d.location,d.company_name,"\
                    "array_to_string(d.company_linkedin_url_array,'|') company_linkedin_url,"\
                    "d.previous_companies,d.education,d.industry,d.summary,d.skills,"\
                    "array_to_string(d.experience_array,'|') experience,"\
                    "array_to_string(d.related_people_array,'|') related_people,"\
                    "array_to_string(d.same_name_people_array,'|') same_name_people,a.linkedin_url as input_url "\
                    "from linkedin_company_base a join company_urls_mapper b on a.linkedin_url=b.alias_url "\
                    " join people_company_mapper c on b.base_url=c.company_url join "\
                    "linkedin_people_base d on c.people_url=d.linkedin_url "\
                    "where a.linkedin_url in %s and d.sub_text ~* '" +  desig_list_reg + "' "
            self.prospect_con.execute(query,(tuple([i[0] for i in urls]),))
            people_prospect_data = self.prospect_con.cursor.fetchall()
            if not people_prospect_data:
                continue
            # insert into crawler base table
            records_list_template = ','.join(['%s']*len(people_prospect_data))
            query = "insert into crawler.linkedin_people_base "\
                "(linkedin_url,name,sub_text,location,company_name,company_linkedin_url,previous_companies,education,"\
                    "industry,summary,skills,experience,related_people,same_name_people,list_id,list_items_url_id) "\
                " VALUES {} ON CONFLICT DO NOTHING ".format(records_list_template)
            insert_list = [list(in_tup) for in_tup in people_prospect_data]
            insert_list1 = [tuple(i[:-1]+[list_id,urls_dict[i[-1]]]) for i in insert_list]
            self.con.cursor.execute(query, insert_list1)
            self.con.commit()

    def prospect_insert_company_website_input(self,domains_all,urls_dict,list_id,desig_list_reg):
        '''search for domains in company table
        :param domains_all: list of tuples. first element should be the domain
        :param urls_dict: key: domain, value: list_items_id
        :param list_id:
        :return:
        '''
        for urls in chunker(domains_all,100):
            # select matching rows
            query = "select distinct b.linkedin_url,b.company_name,b.company_size,b.industry,b.company_type,b.headquarters,b.description,"\
                    "b.founded,b.specialties,b.website,array_to_string(b.employee_details_array,'|') employee_details,"\
                    "array_to_string(b.also_viewed_companies_array,'|') also_viewed_companies,a.website_cleaned "\
                    "from linkedin_company_domains a join linkedin_company_base b on a.linkedin_url=b.linkedin_url "\
                    "where a.website_cleaned in %s "
            self.prospect_con.execute(query,(tuple([i[0] for i in urls]),))
            company_prospect_data_1 = self.prospect_con.cursor.fetchall()
            query = "select distinct b.linkedin_url,b.company_name,b.company_size,b.industry,b.company_type,b.headquarters,b.description,"\
                    "b.founded,b.specialties,b.website,array_to_string(b.employee_details_array,'|') employee_details,"\
                    "array_to_string(b.also_viewed_companies_array,'|') also_viewed_companies,a.domain "\
                    "from linkedin_company_domains a join linkedin_company_base b on a.linkedin_url=b.linkedin_url "\
                    "where  a.domain in %s"
            self.prospect_con.execute(query,(tuple([i[0] for i in urls]),))
            company_prospect_data_2 = self.prospect_con.cursor.fetchall()
            company_prospect_data = list(set(company_prospect_data_1+company_prospect_data_2))
            del company_prospect_data_1,company_prospect_data_2
            logging.info('fetch_prospects: company website - fetched companies : {}'.format(len(company_prospect_data)))
            if not company_prospect_data:
                continue
            # insert linkedin_url into list_items_urls, then get the uuid, then use both to insert. This is done because
            # we don't know the list_items_url_id at this point
            list_items_urls = [(list_id,urls_dict[i[-1]],i[0]) for i in company_prospect_data]
            records_list_template = ','.join(['%s']*len(list_items_urls))
            query = "insert into crawler.list_items_urls "\
                "(list_id,list_items_id,url) "\
                " VALUES {} ON CONFLICT DO NOTHING ".format(records_list_template)
            self.con.cursor.execute(query, list_items_urls)
            self.con.commit()
            query = "select url,id as list_items_url_id from crawler.list_items_urls where "\
                    "list_id = %s and list_items_id in %s"
            self.con.cursor.execute(query, (list_id,tuple([i[1] for i in list_items_urls]),))
            list_items_urls_list = self.con.cursor.fetchall()
            list_items_urls_dict = {}
            for i in list_items_urls_list:
                list_items_urls_dict[i[0]] = i[1]
            # insert into crawler base table
            records_list_template = ','.join(['%s']*len(company_prospect_data))
            query = "insert into crawler.linkedin_company_base "\
                "(linkedin_url,company_name,company_size,industry,company_type,headquarters,description,founded,"\
                    "specialties,website,employee_details,also_viewed_companies,list_id,list_items_url_id) "\
                " VALUES {} ON CONFLICT DO NOTHING ".format(records_list_template)
            insert_list = [list(in_tup) for in_tup in company_prospect_data]
            insert_list1 = [tuple(i[:-1]+[list_id,list_items_urls_dict[i[0]]]) for i in insert_list]
            self.con.cursor.execute(query, insert_list1)
            self.con.commit()
            # insert people for these companies into the people table
            query = "select distinct d.linkedin_url,d.name,d.sub_text,d.location,d.company_name,"\
                    "array_to_string(d.company_linkedin_url_array,'|') company_linkedin_url,"\
                    "d.previous_companies,d.education,d.industry,d.summary,d.skills,"\
                    "array_to_string(d.experience_array,'|') experience,"\
                    "array_to_string(d.related_people_array,'|') related_people,"\
                    "array_to_string(d.same_name_people_array,'|') same_name_people,a.linkedin_url as input_url "\
                    "from linkedin_company_base a join company_urls_mapper b on a.linkedin_url=b.alias_url "\
                    " join people_company_mapper c on b.base_url=c.company_url join "\
                    "linkedin_people_base d on c.people_url=d.linkedin_url "\
                    "where a.linkedin_url in %s and d.sub_text ~* '" +  desig_list_reg + "' "
            self.prospect_con.execute(query,(tuple([i[0] for i in list_items_urls_list]),))
            people_prospect_data = self.prospect_con.cursor.fetchall()
            if not people_prospect_data:
                continue
            # insert into crawler base table
            records_list_template = ','.join(['%s']*len(people_prospect_data))
            query = "insert into crawler.linkedin_people_base "\
                "(linkedin_url,name,sub_text,location,company_name,company_linkedin_url,previous_companies,education,"\
                    "industry,summary,skills,experience,related_people,same_name_people,list_id,list_items_url_id) "\
                " VALUES {} ON CONFLICT DO NOTHING ".format(records_list_template)
            insert_list = [list(in_tup) for in_tup in people_prospect_data]
            insert_list1 = [tuple(i[:-1]+[list_id,list_items_urls_dict[i[-1]]]) for i in insert_list]
            self.con.cursor.execute(query, insert_list1)
            self.con.commit()

    def prospect_insert_people(self,urls_all,urls_dict,list_id,desig_list_reg):
        '''search for urls in people table
        :param urls_all: list of tuples. first element should be the url
        :param urls_dict: key: url, value: list_items_url_id
        :param list_id:
        :return:
        '''
        for urls in chunker(urls_all,100):
            query = "select linkedin_url,name,sub_text,location,company_name,"\
                    "array_to_string(company_linkedin_url_array,'|') company_linkedin_url,"\
                    "previous_companies,education,industry,summary,skills,"\
                    "array_to_string(experience_array,'|') experience,"\
                    "array_to_string(related_people_array,'|') related_people,"\
                    "array_to_string(same_name_people_array,'|') same_name_people from linkedin_people_base "\
                    "where linkedin_url in %s"
            self.prospect_con.execute(query,(tuple([i[0] for i in urls]),))
            people_prospect_data = self.prospect_con.cursor.fetchall()
            logging.info('fetch_prospects: people - fetched people : {}'.format(len(people_prospect_data)))
            if not people_prospect_data:
                continue
            # insert into crawler base table
            records_list_template = ','.join(['%s']*len(people_prospect_data))
            query = "insert into crawler.linkedin_people_base "\
                "(linkedin_url,name,sub_text,location,company_name,company_linkedin_url,previous_companies,education,"\
                    "industry,summary,skills,experience,related_people,same_name_people,list_id,list_items_url_id) "\
                " VALUES {} ON CONFLICT DO NOTHING ".format(records_list_template)
            insert_list = [list(in_tup) for in_tup in people_prospect_data]
            insert_list1 = [tuple(i+[list_id,urls_dict[i[0]]]) for i in insert_list]
            self.con.cursor.execute(query, insert_list1)
            self.con.commit()
            # select companies for these people and insert them
            query = "select b.linkedin_url,b.company_name,b.company_size,b.industry,b.company_type,b.headquarters,b.description,"\
                    "b.founded,b.specialties,b.website,array_to_string(b.employee_details_array,'|') employee_details,"\
                    "array_to_string(b.also_viewed_companies_array,'|') also_viewed_companies, a.linkedin_url as input_url "\
                    "from linkedin_people_base a join people_urls_mapper d on a.linkedin_url=d.alias_url "\
                    " join people_company_mapper c on d.base_url=c.people_url join "\
                    "linkedin_company_base b on c.people_url=b.linkedin_url "\
                    "where a.linkedin_url in %s"
            self.prospect_con.execute(query,(tuple([i[0] for i in urls]),))
            company_prospect_data = self.prospect_con.cursor.fetchall()
            if not company_prospect_data:
                continue
            # insert into crawler base table
            records_list_template = ','.join(['%s']*len(company_prospect_data))
            query = "insert into crawler.linkedin_company_base "\
                "(linkedin_url,company_name,company_size,industry,company_type,headquarters,description,founded,"\
                    "specialties,website,employee_details,also_viewed_companies,list_id,list_items_url_id) "\
                " VALUES {} ON CONFLICT DO NOTHING ".format(records_list_template)
            insert_list = [list(in_tup) for in_tup in company_prospect_data]
            insert_list1 = [tuple(i[:-1]+[list_id,urls_dict[i[-1]]]) for i in insert_list]
            self.con.cursor.execute(query, insert_list1)
            self.con.commit()

    def prospect_insert_from_query(self,prospect_query,list_id,list_items_url_id,desig_list_reg):
        '''
        :param prospect_query: this should be the conditions.all conditions should start with a.
                eg:a."a.location ~* 'united states' and a.industry in ('Information Technology and Services','Computer Software') "
        :return:
        '''
        if not prospect_query:
            raise ValueError('Need to give a query to fetch company details from prospect db')
        query = "select linkedin_url,company_name,company_size,industry,company_type,headquarters,description,"\
                    "founded,specialties,website,array_to_string(employee_details_array,'|') employee_details,"\
                    "array_to_string(also_viewed_companies_array,'|') also_viewed_companies "\
                    "from linkedin_company_base a where "+prospect_query
        self.prospect_con.execute(query)
        company_prospect_data = self.prospect_con.cursor.fetchall()
        logging.info('fetch_prospects: company from query - fetched companies : {}'.format(len(company_prospect_data)))
        if not company_prospect_data:
            return
        # insert into crawler base table
        records_list_template = ','.join(['%s']*len(company_prospect_data))
        query = "insert into crawler.linkedin_company_base "\
            "(linkedin_url,company_name,company_size,industry,company_type,headquarters,description,founded,"\
                "specialties,website,employee_details,also_viewed_companies,list_id,list_items_url_id) "\
            " VALUES {} ON CONFLICT DO NOTHING ".format(records_list_template)
        insert_list = [list(in_tup) for in_tup in company_prospect_data]
        insert_list1 = [tuple(i+[list_id,list_items_url_id]) for i in insert_list]
        self.con.cursor.execute(query, insert_list1)
        self.con.commit()
        # insert people for these companies into the people table
        query = "select d.linkedin_url,d.name,d.sub_text,d.location,d.company_name,"\
                "array_to_string(d.company_linkedin_url_array,'|') company_linkedin_url,"\
                "d.previous_companies,d.education,d.industry,d.summary,d.skills,"\
                "array_to_string(d.experience_array,'|') experience,"\
                "array_to_string(d.related_people_array,'|') related_people,"\
                "array_to_string(d.same_name_people_array,'|') same_name_people,a.linkedin_url as input_url "\
                "from linkedin_company_base a join company_urls_mapper b on a.linkedin_url=b.alias_url "\
                " join people_company_mapper c on b.base_url=c.company_url join "\
                "linkedin_people_base d on c.people_url=d.linkedin_url "\
                "where "+prospect_query+ " and d.sub_text ~* '" +  desig_list_reg + "' "
        self.prospect_con.execute(query)
        people_prospect_data = self.prospect_con.cursor.fetchall()
        # insert into crawler base table
        records_list_template = ','.join(['%s']*len(people_prospect_data))
        query = "insert into crawler.linkedin_people_base "\
            "(linkedin_url,name,sub_text,location,company_name,company_linkedin_url,previous_companies,education,"\
                "industry,summary,skills,experience,related_people,same_name_people,list_id,list_items_url_id) "\
            " VALUES {} ON CONFLICT DO NOTHING ".format(records_list_template)
        insert_list = [list(in_tup) for in_tup in people_prospect_data]
        insert_list1 = [tuple(i[:-1]+[list_id,list_items_url_id]) for i in insert_list]
        self.con.cursor.execute(query, insert_list1)
        self.con.commit()

    def fix_redirect_urls(self,list_id):
        ''' put all urls in the base tables into redirect url tables also
        :param list_id:
        :return:
        '''
        query = " insert into crawler.linkedin_company_redirect_url (url,redirect_url) "\
                " select distinct linkedin_url,linkedin_url from "\
                " crawler.linkedin_company_base where list_id = %s on conflict do nothing "
        self.con.cursor.execute(query,(list_id,))
        query = " insert into crawler.linkedin_people_redirect_url (url,redirect_url) "\
                " select distinct linkedin_url,linkedin_url from "\
                " crawler.linkedin_people_base where list_id = %s on conflict do nothing "
        self.con.cursor.execute(query,(list_id,))
        self.con.commit()
