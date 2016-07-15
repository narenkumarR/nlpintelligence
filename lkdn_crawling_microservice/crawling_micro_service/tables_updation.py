__author__ = 'joswin'

import logging
import postgres_connect

from constants import desig_list_regex

class TableUpdater(object):
    '''
    '''
    def __init__(self):
        self.con = postgres_connect.PostgresConnect()

    def update_tables(self,list_id,desig_list=None,similar_companies=1):
        ''' run the sql file for updation here
        :return:
        '''
        logging.info('started table updation')
        if not list_id:
            raise ValueError('Need list id')
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
        # inserting the companies in the initial list which are not crawled
        logging.info('inserting the companies in the initial list which are not crawled')
        query = "insert into crawler.linkedin_company_urls_to_crawl_priority (url,list_id,list_items_url_id) "\
            "select a.url,a.list_id,a.id as list_items_url_id from crawler.list_items_urls a left join "\
            "crawler.linkedin_company_redirect_url c on a.url = c.url or a.url = c.redirect_url left join "\
            "crawler.linkedin_company_finished_urls b on (a.url=b.url or  b.url=c.url or b.url = c.redirect_url) and (b.list_id=a.list_id) "\
            "where b.url is null and a.list_id = %s on conflict do nothing"
        self.con.cursor.execute(query,(list_id,))
        self.con.commit()

        if similar_companies:
            # inserting also viewed companies into the priority table for the initial list
            logging.info('inserting also viewed companies into the priority table for the initial list')
            query = "drop table if exists crawler.tmp_table"
            self.con.cursor.execute(query)
            self.con.commit()
            query = "create table crawler.tmp_table as "\
                "select unnest(crawler.clean_linkedin_url_array(crawler.extract_related_info(string_to_array(a.also_viewed_companies,'|'),1))) as url, "\
                " a.list_id,a.list_items_url_id "\
                "from crawler.linkedin_company_base a join crawler.list_items_urls b on a.linkedin_url = b.url "\
                "where  also_viewed_companies like '%%linkedin%%' and a.list_id = %s "
            self.con.cursor.execute(query,(list_id,))
            self.con.commit()
            query = "insert into crawler.linkedin_company_urls_to_crawl_priority (url,list_id,list_items_url_id) "\
                "select a.url,a.list_id,a.list_items_url_id from crawler.tmp_table a left join " \
                "crawler.linkedin_company_redirect_url c on a.url = c.url or a.url = c.redirect_url left join "\
                "crawler.linkedin_company_finished_urls b on (a.url = b.url or  b.url=c.url or b.url = c.redirect_url) and (b.list_id=a.list_id) "\
                "where b.url is null and a.list_id = %s on conflict do nothing "
            self.con.cursor.execute(query,(list_id,))
            self.con.commit()

            # insert also viewed companies for rest of the companies in the table
            logging.info('insert also viewed companies for rest of the companies in the table')
            query = "drop table if exists crawler.tmp_table"
            self.con.cursor.execute(query)
            self.con.commit()
            query = "create table crawler.tmp_table as "\
                "select  unnest(crawler.clean_linkedin_url_array(crawler.extract_related_info(string_to_array(a.also_viewed_companies,'|'),1))) as url, "\
                " list_id,list_items_url_id "\
                "from crawler.linkedin_company_base a where  also_viewed_companies like '%%linkedin%%' and list_id = %s "
            self.con.cursor.execute(query,(list_id,))
            self.con.commit()
            query = "insert into crawler.linkedin_company_urls_to_crawl_priority (url,list_id,list_items_url_id) "\
                "select a.url,a.list_id,a.list_items_url_id from crawler.tmp_table a left join "\
                "crawler.linkedin_company_redirect_url c on a.url = c.url or a.url = c.redirect_url left join "\
                "crawler.linkedin_company_finished_urls b on (a.url = b.url or  b.url=c.url or b.url = c.redirect_url) and (b.list_id=a.list_id) "\
                "where b.url is null and a.list_id = %s on conflict do nothing "
            self.con.cursor.execute(query,(list_id,))
            self.con.commit()

            # for people who have the target designations ,put the company urls if they are not crawled already
            logging.info('for people who have the target designations ,put the company urls if they are not crawled already')
            query = "drop table if exists crawler.tmp_table"
            self.con.cursor.execute(query)
            self.con.commit()
            query = "create table crawler.tmp_table as "\
                "select distinct unnest(crawler.clean_linkedin_url_array(crawler.extract_related_info(string_to_array(a.company_linkedin_url,'|'),1))) as url, "\
                " list_id,list_items_url_id "\
                "from crawler.linkedin_people_base a "\
                "where  a.company_linkedin_url like '%%linkedin%%' and list_id = %s and "\
                "regexp_replace(sub_text,'\yin\y|\yof\y|\yat\y',' ') ~* %s "
            self.con.cursor.execute(query,(list_id,desig_list_reg,))
            self.con.commit()
            # import pdb
            # pdb.set_trace()
            query = "insert into crawler.linkedin_company_urls_to_crawl_priority (url,list_id,list_items_url_id) "\
                "select a.url,a.list_id,a.list_items_url_id from crawler.tmp_table a left join "\
                "crawler.linkedin_company_redirect_url c on a.url = c.url or a.url = c.redirect_url left join "\
                "crawler.linkedin_company_finished_urls b on (a.url = b.url or  b.url=c.url or b.url = c.redirect_url) and (a.list_id=b.list_id) "\
                "where b.url is null and a.list_id = %s on conflict do nothing "
            self.con.cursor.execute(query,(list_id,))
            self.con.commit()

        # inserting people details into people priority table (who have valid designation)
        logging.info('inserting people details into people priority table (who have valid designation)')
        query = "drop table if exists crawler.tmp_table "
        self.con.cursor.execute(query)
        self.con.commit()
        query = "create table crawler.tmp_table as "\
                "select distinct unnest(crawler.clean_linkedin_url_array(crawler.extract_related_info(string_to_array(employee_details,'|'),1))) as url, "\
                "unnest(crawler.clean_linkedin_url_array(crawler.extract_related_info(string_to_array(employee_details,'|'),3))) as position, "\
                "list_id, list_items_url_id "\
                "from crawler.linkedin_company_base where employee_details like '%%linkedin%%' and list_id = %s "
        self.con.cursor.execute(query,(list_id,))
        self.con.commit()
        query = "insert into crawler.linkedin_people_urls_to_crawl_priority (url,list_id,list_items_url_id) "\
                "select a.url,a.list_id,a.list_items_url_id from crawler.tmp_table a "\
                "left join crawler.linkedin_people_redirect_url c on a.url = c.url or a.url = c.redirect_url "\
                " left join crawler.linkedin_people_finished_urls b on (b.url=c.url or b.url = c.redirect_url or a.url=b.url) and (a.list_id=b.list_id) "\
                "where b.url is null and regexp_replace(position,'\yin\y|\yof\y|\yat\y',' ') ~* '" +  desig_list_reg +"' "\
                " and a.list_id = %s on conflict do nothing "
        self.con.cursor.execute(query,(list_id,))
        self.con.commit()

        # if similar_companies:
        # inserting people details from related people field (who have valid designation)
        logging.info('inserting people details from related people field (who have valid designation)')
        query = "drop table if exists crawler.tmp_table "
        self.con.cursor.execute(query)
        self.con.commit()
        query = "create table crawler.tmp_table as "\
                "select distinct unnest(crawler.clean_linkedin_url_array(crawler.extract_related_info(string_to_array(related_people,'|'),1))) as url, "\
                "unnest(crawler.clean_linkedin_url_array(crawler.extract_related_info(string_to_array(related_people,'|'),3))) as position, "\
                " list_id,list_items_url_id "\
                "from crawler.linkedin_people_base where related_people like '%%linkedin%%' and list_id = %s"
        self.con.cursor.execute(query,(list_id,))
        self.con.commit()
        query = "insert into crawler.linkedin_people_urls_to_crawl_priority (url,list_id,list_items_url_id) "\
                "select a.url,a.list_id,a.list_items_url_id from crawler.tmp_table a "\
                "left join  crawler.linkedin_people_redirect_url c on a.url = c.url or a.url = c.redirect_url "\
                " left join crawler.linkedin_people_finished_urls b on (b.url=c.url or b.url = c.redirect_url or a.url=b.url) and (a.list_id=b.list_id) "\
                "where b.url is null and regexp_replace(position,'\yin\y|\yof\y|\yat\y',' ') ~* %s "\
                " and a.list_id = %s on conflict do nothing "
        self.con.cursor.execute(query,(desig_list_reg,list_id,))
        self.con.commit()

        # next insert all people to the urls to crawl list (not priority). This is to get the related people for these guys
        # in cases where there is not enough urls to crawl
        query = "drop table if exists crawler.tmp_table "
        self.con.cursor.execute(query)
        self.con.commit()
        query = "create table crawler.tmp_table as "\
                "select distinct unnest(crawler.clean_linkedin_url_array(crawler.extract_related_info(string_to_array(related_people,'|'),1))) as url, "\
                "unnest(crawler.clean_linkedin_url_array(crawler.extract_related_info(string_to_array(related_people,'|'),3))) as position, "\
                " list_id,list_items_url_id "\
                "from crawler.linkedin_people_base where related_people like '%%linkedin%%' and list_id = %s"
        self.con.cursor.execute(query,(list_id,))
        self.con.commit()
        query = "insert into crawler.linkedin_people_urls_to_crawl (url,list_id,list_items_url_id) "\
                "select a.url,a.list_id,a.list_items_url_id from crawler.tmp_table a "\
                "left join  crawler.linkedin_people_redirect_url c on a.url = c.url or a.url = c.redirect_url "\
                " left join crawler.linkedin_people_finished_urls b on (b.url=c.url or b.url = c.redirect_url or a.url=b.url) and (a.list_id=b.list_id) "\
                "where b.url is null  "\
                " and a.list_id = %s on conflict do nothing "
        self.con.cursor.execute(query,(list_id,))
        self.con.commit()

        # deleting invalid urls
        logging.info('deleting invalid urls')
        query = "delete from crawler.linkedin_company_urls_to_crawl_priority "\
                "where url not like '%%linkedin%%' or url like '%%,%%' or url like '%%|%%' or url like '%%{}%%' "
        self.con.cursor.execute(query)
        query = "delete from crawler.linkedin_people_urls_to_crawl_priority "\
                "where url not like '%%linkedin%%' or url like '%%,%%' or url like '%%|%%' or url like '%%{}%%' "
        self.con.cursor.execute(query)
        query = "delete from crawler.linkedin_people_urls_to_crawl "\
                "where url not like '%%linkedin%%' or url like '%%,%%' or url like '%%|%%' or url like '%%{}%%' "
        self.con.cursor.execute(query)
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

        # commiting the results
        self.con.commit()
        self.con.close_cursor()
        logging.info('finished table updation')
