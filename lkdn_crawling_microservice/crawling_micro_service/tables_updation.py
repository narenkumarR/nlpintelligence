__author__ = 'joswin'

import logging
import postgres_connect

from constants import desig_list_regex

class TableUpdater(object):
    '''
    '''
    def __init__(self):
        self.con = postgres_connect.PostgresConnect()

    def update_tables(self,list_id,desig_list=None,similar_companies=1,company_select_query = ''):
        '''
        :param list_id:
        :param desig_list:
        :param similar_companies:
        :param company_select_query: give condition for company selection as query " a.headquarters like '%united states%' and .."
                if this will be used for selecting people
        :return:
        '''
        logging.info('started table updation')
        if not list_id:
            raise ValueError('Need list id')
        table_name_id = str(list_id).split('-')[0]
        if not desig_list:
            desig_list_reg = desig_list_regex
        else:
            desig_list_reg = '\y' + '\y|\y'.join(desig_list) + '\y'
        self.con.get_cursor()
        if not similar_companies:
            logging.info('Deleting from urls to crawl tables, since we are not looking for similar companies')
            self.con.cursor.execute('delete from {} where list_id=%s'.format('crawler.linkedin_people_urls_to_crawl'),(list_id,))
            self.con.cursor.execute('delete from {} where list_id=%s'.format('crawler.linkedin_company_urls_to_crawl'),(list_id,))
            self.con.cursor.execute('delete from {} where list_id=%s'.format('crawler.linkedin_people_urls_to_crawl_priority'),(list_id,))
            self.con.cursor.execute('delete from {} where list_id=%s'.format('crawler.linkedin_company_urls_to_crawl_priority'),(list_id,))
            self.con.commit()
        # inserting the urls in the initial list which are not crawled
        logging.info('inserting the companies in the initial list which are not crawled')
        # companies
        query = "drop table if exists crawler.tmp_table1_updation_{}".format(table_name_id)
        self.con.cursor.execute(query)
        self.con.commit()
        query = "create table crawler.tmp_table1_updation_{} as "\
            "select a.url,a.list_id,a.id as list_items_url_id from crawler.list_items_urls a  join "\
            "crawler.linkedin_company_base b on ( b.linkedin_url = a.url) and (b.list_id=a.list_id) "\
            "where a.list_id = %s and (a.url like '%%/company/%%' or a.url like '%%/companies/%%')".format(table_name_id)
            # "crawler.linkedin_company_redirect_url c on a.url = c.url join "\
            #"crawler.linkedin_company_base b on ( b.linkedin_url = c.redirect_url) and (b.list_id=a.list_id) "\
        self.con.cursor.execute(query,(list_id,))
        self.con.commit()
        query = "insert into crawler.linkedin_company_urls_to_crawl_priority (url,list_id,list_items_url_id) "\
                "select split_part(a.url,'?trk',1),a.list_id,a.id from crawler.list_items_urls a "\
                " left join crawler.tmp_table1_updation_{} b on a.url = b.url where b.url is null "\
                " and (a.url like '%%/company/%%' or a.url like '%%/companies/%%') and a.list_id = %s "\
                " on conflict do nothing".format(table_name_id)
        self.con.cursor.execute(query,(list_id,))
        self.con.commit()
        # people
        query = "drop table if exists crawler.tmp_table1_updation_{}".format(table_name_id)
        self.con.cursor.execute(query)
        self.con.commit()
        query = "create table crawler.tmp_table1_updation_{} as "\
            "select a.url,a.list_id,a.id as list_items_url_id from crawler.list_items_urls a  join "\
            "crawler.linkedin_people_base b on (  b.linkedin_url = a.url) and (b.list_id=a.list_id) "\
            "where a.list_id = %s and (a.url like '%%/pub/%%' or a.url like '%%/profile/%%' or a.url like '%%/in/%%')".format(table_name_id)
        # "crawler.linkedin_people_redirect_url c on a.url = c.url  join "\
        self.con.cursor.execute(query,(list_id,))
        self.con.commit()
        query = "insert into crawler.linkedin_people_urls_to_crawl_priority (url,list_id,list_items_url_id) "\
                "select split_part(a.url,'?trk',1),a.list_id,a.id from crawler.list_items_urls a "\
                " left join crawler.tmp_table1_updation_{} b on a.url = b.url where b.url is null "\
                " and (a.url like '%%/pub/%%' or a.url like '%%/profile/%%' or a.url like '%%/in/%%') and a.list_id = %s "\
                " on conflict do nothing".format(table_name_id)
        self.con.cursor.execute(query,(list_id,))
        self.con.commit()
        # for people in the initial list, insert their company urls if not present
        logging.info('for people in the initial url list ,put the company urls if they are not crawled already')
        query = "drop table if exists crawler.tmp_table_updation_{}".format(table_name_id)
        self.con.cursor.execute(query)
        self.con.commit()
        query = "create table crawler.tmp_table_updation_{} as "\
            "select distinct " \
            " unnest(crawler.clean_linkedin_url_array(crawler.extract_related_info(string_to_array(a.company_linkedin_url,'|'),1))) as url, "\
            " a.list_id,a.list_items_url_id "\
            "from crawler.linkedin_people_base a  "\
            " join crawler.list_items_urls b on a.list_id=b.list_id and (a.linkedin_url = b.url ) "\
            "where  a.company_linkedin_url like '%%linkedin%%' and a.list_id = %s "\
            "  ".format(table_name_id)
        # " join  crawler.linkedin_people_redirect_url c on a.linkedin_url = c.redirect_url "\
        self.con.cursor.execute(query,(list_id,))
        self.con.commit()
        # import pdb
        # pdb.set_trace()
        query = "drop table if exists crawler.tmp_table1_updation_{} ".format(table_name_id)
        self.con.cursor.execute(query)
        self.con.commit()
        query = "create table crawler.tmp_table1_updation_{} as "\
            "select a.url,a.list_id,a.list_items_url_id  "\
            "from crawler.tmp_table_updation_{} a join  crawler.linkedin_company_redirect_url c on a.url = c.url  "\
            "join crawler.linkedin_company_base b on ( b.linkedin_url = a.url ) and (a.list_id=b.list_id) "\
            "where  a.list_id = %s".format(table_name_id,table_name_id)
        # "from crawler.tmp_table_updation_{} a join  crawler.linkedin_company_redirect_url c on a.url = c.url  "\
        self.con.cursor.execute(query,(list_id,))
        self.con.commit()
        query = "insert into crawler.linkedin_company_urls_to_crawl_priority (url,list_id,list_items_url_id) "\
                "select split_part(a.url,'?trk',1),a.list_id,a.list_items_url_id from crawler.tmp_table_updation_{} a "\
                " left join crawler.tmp_table1_updation_{} b on a.url = b.url where b.url is null "\
                " and (a.url like '%/company/%' or a.url like '%/companies/%') "\
                " on conflict do nothing".format(table_name_id,table_name_id)
        self.con.cursor.execute(query)
        self.con.commit()

        if similar_companies:
            # inserting also viewed companies into the priority table for the initial list (here not giving company selection query)
            logging.info('inserting also viewed companies into the priority table for the initial list')
            query = "drop table if exists crawler.tmp_table_updation_{}".format(table_name_id)
            self.con.cursor.execute(query)
            self.con.commit()
            query = "create table crawler.tmp_table_updation_{} as "\
                "select unnest(crawler.clean_linkedin_url_array(crawler.extract_related_info(string_to_array(a.also_viewed_companies,'|'),1))) as url, "\
                " a.list_id,a.list_items_url_id "\
                "from crawler.linkedin_company_base a "\
                    " join crawler.list_items_urls b on a.list_id=b.list_id and (a.linkedin_url = b.url ) "\
                "where  also_viewed_companies like '%%linkedin%%' and a.list_id = %s ".format(table_name_id)
            # " join  crawler.linkedin_company_redirect_url c on a.linkedin_url = c.url "\
            self.con.cursor.execute(query,(list_id,))
            self.con.commit()
            query = "drop table if exists crawler.tmp_table1_updation_{} ".format(table_name_id)
            self.con.cursor.execute(query)
            self.con.commit()
            query = "create table crawler.tmp_table1_updation_{} as "\
                "select a.url,a.list_id,a.list_items_url_id  "\
                "from crawler.tmp_table_updation_{} a " \
                " join  crawler.linkedin_company_redirect_url c on a.url = c.url  "\
                "join crawler.linkedin_company_base b on (b.linkedin_url = c.redirect_url) and (a.list_id=b.list_id) "\
                "where  a.list_id = %s ".format(table_name_id,table_name_id)
            self.con.cursor.execute(query,(list_id,))
            self.con.commit()
            query = "insert into crawler.linkedin_company_urls_to_crawl_priority (url,list_id,list_items_url_id) "\
                    "select split_part(a.url,'?trk',1),a.list_id,a.list_items_url_id from crawler.tmp_table_updation_{} a "\
                    " left join crawler.tmp_table1_updation_{} b "\
                    "on a.url = b.url where b.url is null and (a.url like '%/company/%' or a.url like '%/companies/%') "\
                    " on conflict do nothing".format(table_name_id,table_name_id)
            self.con.cursor.execute(query)
            self.con.commit()

            # insert also viewed companies for rest of the companies in the table (company search query used here)
            logging.info('insert also viewed companies for rest of the companies in the table')
            query = "drop table if exists crawler.tmp_table_updation_{}".format(table_name_id)
            self.con.cursor.execute(query)
            self.con.commit()
            query = "create table crawler.tmp_table_updation_{} as "\
                "select  unnest(crawler.clean_linkedin_url_array(crawler.extract_related_info(string_to_array(a.also_viewed_companies,'|'),1))) as url, "\
                " list_id,list_items_url_id "\
                "from crawler.linkedin_company_base a where  also_viewed_companies like '%%linkedin%%' and list_id = %s ".format(table_name_id)
            if company_select_query:
                query = query + " and "+ company_select_query
            self.con.cursor.execute(query,(list_id,))
            self.con.commit()
            query = "drop table if exists crawler.tmp_table1_updation_{} ".format(table_name_id)
            self.con.cursor.execute(query)
            self.con.commit()
            query = "create table crawler.tmp_table1_updation_{} as "\
                "select a.url,a.list_id,a.list_items_url_id  "\
                "from crawler.tmp_table_updation_{} a join  crawler.linkedin_company_redirect_url c on a.url = c.url  "\
                "join crawler.linkedin_company_base b on (b.linkedin_url = c.redirect_url) and (a.list_id=b.list_id) "\
                "where  a.list_id = %s".format(table_name_id,table_name_id)
            self.con.cursor.execute(query,(list_id,))
            self.con.commit()
            query = "insert into crawler.linkedin_company_urls_to_crawl_priority (url,list_id,list_items_url_id) "\
                    "select split_part(a.url,'?trk',1),a.list_id,a.list_items_url_id from crawler.tmp_table_updation_{} a "\
                    " left join crawler.tmp_table1_updation_{} b "\
                    "on a.url = b.url where b.url is null and (a.url like '%/company/%' or a.url like '%/companies/%') "\
                    " on conflict do nothing".format(table_name_id,table_name_id)
            self.con.cursor.execute(query)
            self.con.commit()

            # for people who have the target designations ,put the company urls if they are not crawled already
            logging.info('for people who have the target designations ,put the company urls if they are not crawled already')
            query = "drop table if exists crawler.tmp_table_updation_{}".format(table_name_id)
            self.con.cursor.execute(query)
            self.con.commit()
            query = "create table crawler.tmp_table_updation_{} as "\
                "select distinct unnest(crawler.clean_linkedin_url_array(crawler.extract_related_info(string_to_array(a.company_linkedin_url,'|'),1))) as url, "\
                " list_id,list_items_url_id "\
                "from crawler.linkedin_people_base a "\
                "where  a.company_linkedin_url like '%%linkedin%%' and list_id = %s and "\
                "regexp_replace(sub_text,'\yin\y|\yof\y|\yat\y',' ') ~* %s ".format(table_name_id)
            self.con.cursor.execute(query,(list_id,desig_list_reg,))
            self.con.commit()
            # import pdb
            # pdb.set_trace()
            query = "drop table if exists crawler.tmp_table1_updation_{} ".format(table_name_id)
            self.con.cursor.execute(query)
            self.con.commit()
            query = "create table crawler.tmp_table1_updation_{} as "\
                "select a.url,a.list_id,a.list_items_url_id  "\
                "from crawler.tmp_table_updation_{} a join  crawler.linkedin_company_redirect_url c on a.url = c.url  "\
                "join crawler.linkedin_company_base b on ( b.linkedin_url = c.redirect_url ) and (a.list_id=b.list_id) "\
                "where  a.list_id = %s".format(table_name_id,table_name_id)
            self.con.cursor.execute(query,(list_id,))
            self.con.commit()
            query = "insert into crawler.linkedin_company_urls_to_crawl_priority (url,list_id,list_items_url_id) "\
                    "select split_part(a.url,'?trk',1),a.list_id,a.list_items_url_id from crawler.tmp_table_updation_{} a "\
                    " left join crawler.tmp_table1_updation_{} b on a.url = b.url where b.url is null "\
                    " and (a.url like '%/company/%' or a.url like '%/companies/%') "\
                    " on conflict do nothing".format(table_name_id,table_name_id)
            self.con.cursor.execute(query)
            self.con.commit()

        # inserting people details into people priority table (who have valid designation)
        logging.info('inserting people details into people priority table (who have valid designation)')
        query = "drop table if exists crawler.tmp_table_updation_{} ".format(table_name_id)
        self.con.cursor.execute(query)
        self.con.commit()
        logging.info('creating tmp_table_updation')
        query = "create table crawler.tmp_table_updation_{} as "\
                "select distinct unnest(crawler.clean_linkedin_url_array(crawler.extract_related_info(string_to_array(employee_details,'|'),1))) as url, "\
                "unnest(crawler.clean_linkedin_url_array(crawler.extract_related_info(string_to_array(employee_details,'|'),3))) as position, "\
                "linkedin_url as company_linkedin_url,list_id, list_items_url_id "\
                "from crawler.linkedin_company_base a where employee_details like '%%linkedin%%' and list_id = %s ".format(table_name_id)
        if company_select_query:
            query = query + " and "+company_select_query
        self.con.cursor.execute(query,(list_id,))
        self.con.commit()
        self.con.execute('analyze crawler.tmp_table_updation_{}'.format(table_name_id))
        self.con.commit()
        query = "drop table if exists crawler.tmp_table1_updation_{} ".format(table_name_id)
        self.con.cursor.execute(query)
        self.con.commit()
        logging.info('creating tmp_table1_updation')
        query = "create table crawler.tmp_table1_updation_{} as "\
                "select a.url,a.list_id,a.list_items_url_id  "\
                "from crawler.tmp_table_updation_{} a join  crawler.linkedin_people_redirect_url c on a.url = c.url "\
                "join crawler.linkedin_people_base b on ( b.linkedin_url = c.redirect_url ) and (a.list_id=b.list_id) "\
                "where  a.list_id = %s".format(table_name_id,table_name_id)
        self.con.cursor.execute(query,(list_id,))
        self.con.commit()
        self.con.execute('analyze crawler.tmp_table1_updation_{}'.format(table_name_id))
        self.con.commit()
        if not similar_companies:
            query = "delete from crawler.tmp_table_updation_{}  where "\
                    " company_linkedin_url not in (select url from crawler.list_items_urls where list_id = %s) and " \
                    " company_linkedin_url not in (select redirect_url from crawler.list_items_urls a " \
                    " join crawler.linkedin_company_redirect_url b using(url) where a.list_id = %s) ".format(table_name_id)
            self.con.execute(query,(list_id,list_id,))
            self.con.commit()
        query = "insert into crawler.linkedin_people_urls_to_crawl (url,list_id,list_items_url_id) "\
                "select split_part(a.url,'?trk',1),a.list_id,a.list_items_url_id from crawler.tmp_table_updation_{} a "\
                " left join crawler.tmp_table1_updation_{} b on a.url = b.url "\
                " where b.url is null and (a.url like '%/pub/%' or a.url like '%/profile/%' or a.url like '%/in/%')"\
                " on conflict do nothing".format(table_name_id,table_name_id)
        logging.info('inserting to people_urls_to_crawl')
        self.con.cursor.execute(query)
        self.con.commit()
        query = "insert into crawler.linkedin_people_urls_to_crawl_priority (url,list_id,list_items_url_id) "\
                "select split_part(a.url,'?trk',1),a.list_id,a.list_items_url_id from crawler.tmp_table_updation_{} a "\
                " left join crawler.tmp_table1_updation_{} b on a.url = b.url "\
                " where b.url is null and a.position ~* %s "\
                " and (a.url like '%%/pub/%%' or a.url like '%%/profile/%%' or a.url like '%%/in/%%') "\
                " on conflict do nothing".format(table_name_id,table_name_id)
        logging.info('inserting to people_urls_to_crawl_priority')
        self.con.cursor.execute(query,(desig_list_reg,))
        self.con.commit()
        # if similar_companies:
        # inserting people details from related people field (who have valid designation)
        logging.info('inserting people details from related people field (who have valid designation)')
        query = "drop table if exists crawler.tmp_table_updation_{} ".format(table_name_id)
        self.con.cursor.execute(query)
        self.con.commit()
        logging.info('creating tmp_table_updation')
        query = "create table crawler.tmp_table_updation_{} as "\
                "select distinct unnest(crawler.clean_linkedin_url_array(crawler.extract_related_info(string_to_array(related_people,'|'),1))) as url, "\
                "unnest(crawler.clean_linkedin_url_array(crawler.extract_related_info(string_to_array(related_people,'|'),3))) as position, "\
                " string_to_array(company_linkedin_url,'|') as company_linkedin_url,list_id,list_items_url_id "\
                "from crawler.linkedin_people_base where related_people like '%%linkedin%%' and list_id = %s".format(table_name_id)
        self.con.cursor.execute(query,(list_id,))
        self.con.commit()
        self.con.execute('analyze crawler.tmp_table_updation_{}'.format(table_name_id))
        self.con.commit()
        # all these similar people will be put into urls_to_crawl table(not priority).
        # This will give urls to crawl even if there is not enough urls in priority table
        query = "drop table if exists crawler.tmp_table1_updation_{} ".format(table_name_id)
        self.con.cursor.execute(query)
        self.con.commit()
        query = "create table crawler.tmp_table1_updation_{} as "\
                "select a.url,a.list_id,a.list_items_url_id  "\
                "from crawler.tmp_table_updation_{} a join  crawler.linkedin_people_redirect_url c on a.url = c.url  "\
                "join crawler.linkedin_people_base b on ( b.linkedin_url = c.redirect_url ) and (a.list_id=b.list_id) "\
                "where  a.list_id = %s".format(table_name_id,table_name_id)
        logging.info('creating tmp_table_updation1')
        self.con.cursor.execute(query,(list_id,))
        self.con.commit()
        self.con.execute('analyze crawler.tmp_table1_updation_{}'.format(table_name_id))
        self.con.commit()
        if not similar_companies: #not optimal, because in people page, company url is always name, but sometime the url we give can be number based
            query = "delete from crawler.tmp_table_updation_{} where " \
                    " not((company_linkedin_url && any(select array_agg(url) from crawler.list_items_urls where list_id = %s))  or " \
                    " (company_linkedin_url && (select array_agg(redirect_url) from crawler.list_items_urls a join " \
                    " crawler.linkedin_company_redirect_url b using(url) where a.list_id = %s)) )".format(table_name_id)
            self.con.execute(query,(list_id,list_id,))
            self.con.commit()
        query = "insert into crawler.linkedin_people_urls_to_crawl (url,list_id,list_items_url_id) "\
                "select split_part(a.url,'?trk',1),a.list_id,a.list_items_url_id from crawler.tmp_table_updation_{} a "\
                " left join crawler.tmp_table1_updation_{} b on a.url = b.url "\
                " where b.url is null and (a.url like '%/pub/%' or a.url like '%/profile/%' or a.url like '%/in/%') "\
                " on conflict do nothing".format(table_name_id,table_name_id)
        logging.info('inserting to people_urls_to_crawl')
        self.con.cursor.execute(query)
        self.con.commit()
        # put matching people into urls_to_crawl_priority table
        query = "insert into crawler.linkedin_people_urls_to_crawl_priority (url,list_id,list_items_url_id) "\
                "select split_part(a.url,'?trk',1),a.list_id,a.list_items_url_id from crawler.tmp_table_updation_{} a "\
                " left join crawler.tmp_table1_updation_{} b on a.url = b.url where b.url is null "\
                " and a.position ~* %s "\
                " and (a.url like '%%/pub/%%' or a.url like '%%/profile/%%' or a.url like '%%/in/%%') "\
                " on conflict do nothing".format(table_name_id,table_name_id)
        logging.info('inserting to people_urls_to_crawl_priority')
        self.con.cursor.execute(query,(desig_list_reg,))
        self.con.commit()

        # deleting invalid urls (not needed, taking too much time)
        # logging.info('deleting invalid urls')
        # query = "delete from crawler.linkedin_company_urls_to_crawl_priority "\
        #         "where list_id = %s and (url not like '%%linkedin%%' or url like '%%,%%' or url like '%%|%%' or url like '%%{}%%'  "\
        #         " or (url not like '%%/company/%%' and url not like '%%/companies/%%'))"
        # self.con.cursor.execute(query,(list_id,))
        # query = "delete from crawler.linkedin_company_urls_to_crawl "\
        #         "where list_id = %s and (url not like '%%linkedin%%' or url like '%%,%%' or url like '%%|%%' or url like '%%{}%%' "\
        #         " or (url not like '%%/company/%%' and url not like '%%/companies/%%'))"
        # self.con.cursor.execute(query,(list_id,))
        # query = "delete from crawler.linkedin_people_urls_to_crawl_priority "\
        #         "where list_id = %s and (url not like '%%linkedin%%' or url like '%%,%%' or url like '%%|%%' or url like '%%{}%%' "\
        #         " or (url not like '%%/in/%%' and url not like '%%/pub/%%'))"
        # self.con.cursor.execute(query,(list_id,))
        # query = "delete from crawler.linkedin_people_urls_to_crawl "\
        #         "where list_id = %s and (url not like '%%linkedin%%' or url like '%%,%%' or url like '%%|%%' or url like '%%{}%%' "\
        #         " or (url not like '%%/in/%%' and url not like '%%/pub/%%'))"
        # self.con.cursor.execute(query,(list_id,))
        # # insert into finished urls tables (Why? already done while crawling)
        # query = "insert into crawler.linkedin_company_finished_urls (url,list_id,list_items_url_id) "\
        #         "select linkedin_url,list_id,list_items_url_id from crawler.linkedin_company_base "\
        #         "where list_id = %s on conflict do nothing"
        # self.con.cursor.execute(query,(list_id,))
        # query = "insert into crawler.linkedin_people_finished_urls (url,list_id,list_items_url_id) "\
        #         "select linkedin_url,list_id,list_items_url_id from crawler.linkedin_people_base "\
        #         "where list_id = %s on conflict do nothing"
        # self.con.cursor.execute(query,(list_id,))
        # self.con.commit()

        # deleting tmp tables
        query = "drop table if exists crawler.tmp_table_updation_{}".format(table_name_id)
        self.con.cursor.execute(query)
        query = "drop table if exists crawler.tmp_table1_updation_{}".format(table_name_id)
        self.con.cursor.execute(query)
        self.con.commit()
        # final updation
        # logging.info('updating urls ')
        # query = "update crawler.linkedin_company_urls_to_crawl_priority "\
        #         "set url = split_part(split_part(url,'/careers',1),'?trk',1) "
        # self.con.cursor.execute(query)
        # query = "update crawler.linkedin_people_urls_to_crawl_priority "\
        #         "set url = split_part(url,'?trk',1) "
        # self.con.cursor.execute(query)
        # query = "update crawler.linkedin_company_urls_to_crawl "\
        #         "set url = split_part(split_part(url,'/careers',1),'?trk',1) "
        # self.con.cursor.execute(query)
        # query = "update crawler.linkedin_people_urls_to_crawl "\
        #         "set url = split_part(url,'?trk',1) "
        # self.con.cursor.execute(query)

        # using finished_urls db tables finally to remove already crawled urls
        # option to add these urls into the result need to be done
        # query = "insert into crawler.linkedin_company_finished_urls_final (linkedin_url_name) "\
        #         " select split_part(url,'linkedin.com/',2) from crawler.linkedin_company_base "\
        #         " where list_id = %s on conflict do nothing"
        # self.con.cursor.execute(query,(list_id,))
        # query = "insert into crawler.linkedin_people_finished_urls_final (linkedin_url_name) "\
        #         " select split_part(url,'linkedin.com/',2) from crawler.linkedin_people_base "\
        #         " where list_id = %s on conflict do nothing"
        # self.con.cursor.execute(query,(list_id,))
        # self.con.commit()
        # query = "delete from crawler.linkedin_company_urls_to_crawl_priority a "\
        #         " using crawler.linkedin_company_finished_urls_final b "\
        #         " where split_part(a.url,'linkedin.com/',2) = b.linkedin_url_name and a.list_id=%s"
        # self.con.cursor.execute(query,(list_id,))
        # query = "delete from crawler.linkedin_company_urls_to_crawl_priority a "\
        #         " using crawler.linkedin_company_finished_urls_final b "\
        #         " where split_part(a.url,'linkedin.com/',2) = b.linkedin_url_name and a.list_id=%s"
        # self.con.cursor.execute(query,(list_id,))
        # query = "delete from crawler.linkedin_company_urls_to_crawl_priority a "\
        #         " using crawler.linkedin_company_finished_urls_final b "\
        #         " where split_part(a.url,'linkedin.com/',2) = b.linkedin_url_name and a.list_id=%s"
        # self.con.cursor.execute(query,(list_id,))
        # query = "delete from crawler.linkedin_company_urls_to_crawl_priority a "\
        #         " using crawler.linkedin_company_finished_urls_final b "\
        #         " where split_part(a.url,'linkedin.com/',2) = b.linkedin_url_name and a.list_id=%s"
        # self.con.cursor.execute(query,(list_id,))

        # commiting the results
        # self.con.commit()
        self.con.close_cursor()
        logging.info('finished table updation')
