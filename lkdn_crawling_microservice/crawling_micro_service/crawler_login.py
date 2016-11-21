__author__ = 'joswin'

import re
import gc
from Queue import Queue
import threading
import logging
import time
import os
from random import randint

import linkedin_company_crawler_login,linkedin_profile_crawler_login
from proxy_generator import ProxyGen
from postgres_connect import PostgresConnect
import pdb

linkedin_url_clean_regex=r'\?trk=pub-pbmap|\?trk=prof-samename-picture|\?trk=extra_biz_viewers_viewed|\?trk=biz_employee_pub|\?trk=ppro.cprof'

class LinkedinLoginCrawlerThread(object):
    def __init__(self,browser='Firefox',visible=True,proxy=False,use_tor=False):
        '''
        :param browser:
        :param visible:
        :param proxy:
        :param use_tor:
        :param use_db:
        :return:
        '''
        # if login:
        #     raise ValueError('The use of logging-in is to fetch people in a company. This is not implemented yet. Give login =0  for now')
        self.browser = browser
        self.visible = visible
        self.proxy = False #no need to proxy here
        self.ip_matcher = re.compile("^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")
        self.use_tor = use_tor
        self.con = PostgresConnect()
        self.con_check = PostgresConnect() # this is for use in worker_fetch_url function. For some reason, two calls
        # to db from different functions causing problem. Need to investigate properly
        self.table_fields_company = ['Linkedin URL','Company Name','Company Size','Industry','Type','Headquarters',
                             'Description Text','Founded','Specialties','Website'
            ,'Employee Details','Also Viewed Companies','employee_count_linkedin','list_id','list_items_url_id']
        self.table_field_names_company = ['linkedin_url','company_name','company_size','industry','company_type','headquarters',
                                  'description','founded','specialties','website','employee_details',
                                  'also_viewed_companies','employee_count_linkedin',
                                  'list_id','list_items_url_id']
        # following not needed because we are not crawling people pages after loggin in currently
        # self.table_fields_people = ['Linkedin URL','Name','Position','Location','Company','CompanyLinkedinPage',
        #                          'PreviousCompanies','Education','Industry','Summary','Skills','Experience'
        #                          ,'Related People','Same Name People','list_id','list_items_url_id']
        # self.table_field_names_people = ['linkedin_url','name','sub_text','location','company_name','company_linkedin_url',
        #                           'previous_companies','education','industry','summary','skills','experience',
        #                           'related_people','same_name_people','list_id','list_items_url_id']
        if proxy:
            self.proxy_generator = ProxyGen(browser_name=browser,visible=visible,page_load_timeout=60)
            self.proxies = Queue(maxsize=0)
            # self.proxies.put((None,None)) #try with actual ip first time
            # self.gen_proxies() # logging problem if this runs before init. put this in run call

    def gen_proxies(self):
        '''
        :return:
        '''
        logging.info('company part login: trying to get proxies')
        if not self.proxy:
            return [(None,None)]
        else:
            try:
                self.proxy_generator.activate_browser()
                proxies = self.proxy_generator.generate_proxy()
            except Exception :
                logging.exception('company part login: could not create proxies. using None')
                proxies = [(None,None)]
            try:
                self.proxy_generator.exit()
            except:
                pass
        logging.info('company part login: Proxies fetched {}'.format(proxies))
        for i in proxies:
            if i[0] is not None:
                if self.ip_matcher.match(i[0]):
                    self.proxies.put(i)
        # logging.info('All Proxies fetched {}'.format(self.proxies)) #not printing proxy list. only object name

    def get_proxy(self):
        ''' call this when a proxy is needed
        :return:
        '''
        if self.proxies.empty():
            self.gen_proxies()
            if self.proxies.empty():
                return (None,None)
        return self.proxies.get()

    def worker_fetch_url(self):
        '''
        :return:
        '''
        if self.proxy:
            try: #some error happens while getting proxy sometimes. putting it in try
                logging.info('company part login: No proxies in queue,trying to get proxies')
                proxy_dets = self.get_proxy()
                logging.info('company part login: proxy to be used : {}'.format(proxy_dets))
                proxy_ip,proxy_port = proxy_dets[0],proxy_dets[1]

            except: #if  coming here, run without proxy
                logging.exception('company part login: Error while getting proxy. Running without proxy ')
                proxy_ip, proxy_port = None,None
        else:
            proxy_ip, proxy_port,proxy_dets = None,None,None
        crawler_company = linkedin_company_crawler_login.LinkedinOrganizationService(self.browser,self.visible,proxy=False,
                                                                  proxy_ip=proxy_ip,proxy_port=proxy_port,
                                                                  use_tor=self.use_tor)
        # crawler_people = linkedin_profile_crawler_login.LinkedinProfileCrawler()
        def get_output_company(crawler_company,url,res_1):
            res_1['result'] = crawler_company.get_organization_details_from_linkedin_link(url,
                                                                                          designations=self.desig_list,
                                                                                          next_page=self.no_pages_to_search)
            
        # def get_output_people(crawler_company,crawler_people,url,res_1):
        #     '''crawler_company has the browser. crawler_people processes the soup '''
        #     soup = crawler_company.link_parser.get_soup(url)
        #     res_1['result'] = crawler_people.fetch_details_soupinput(soup)
        #     res_1['Linkedin URL'] = crawler_company.link_parser.browser.current_url
        #     res_1['Original URL'] = url
        
        no_errors = 0
        ind = 0
        n_blocks =0
        # self.crawler_queue.put(crawler)
        while self.run_queue:
            # we need to crawl company and people simultaneously. after crawling a company page, those urls are put into
            # in_queue_people, and using same browser we will crawl those urls. after each iteration, check if no_errors
            # is 0. if 0 and in_queue_people is not empty, fetch one url from it and run
            ind += 1
            if not self.in_queue_company.empty():
                # import pdb
                # pdb.set_trace()
                url,list_items_url_id = self.in_queue_company.get()
                # checking if the url is already crawled
                query = "select linkedin_url from {} where list_id = %s and linkedin_url = %s limit 1".format(self.company_base_table)
                try:
                    # self.con.get_cursor()
                    self.con_check.execute(query,(self.list_id,url,))
                    url_pres = self.con_check.cursor.fetchall()
                except:
                    url_pres = []
                # self.con.close_cursor()
                if url_pres:
                    self.in_queue_company.task_done()
                    continue
                # if url in self.processed_queue.queue or url in self.error_queue.queue:
                #     continue
                try:
                    for t_no in range(2):
                        time.sleep(randint(5,10))
                        logging.info('company part login: Input URL:{}, thread:{}, try:{}'.format(url,threading.currentThread(),t_no+1))
                        res_1 = {}
                        # pdb.set_trace()
                        get_output_company(crawler_company,url,res_1)
                        if res_1.get('result','error_happened') != 'error_happened':
                            res = res_1['result']
                            if res.get('Company Name','LinkedIn') != 'LinkedIn':
                                break
                    if 'result' in res_1:
                        res = res_1['result']
                        if res:
                            if 'Company Name' in res :
                                if res['Company Name'] and (res['Company Name'] != 'LinkedIn' or url.startswith('https://www.linkedin.com/company/1337') or url.startswith('https://www.linkedin.com/company/linkedin')):
                                    self.out_queue_company.put((res,list_items_url_id))
                                    # self.processed_queue.put(url)
                                    no_errors = 0
                                    n_blocks = 0
                                    # if 'Employee Details' in res:
                                    #     # put all the people urls into in_queue_people
                                    #     for emp_dets in res['Employee Details']:
                                    #         if emp_dets.get('linkedin_url','') and emp_dets.get('Name','')\
                                    #                 and not re.search('LinkedIn Member',emp_dets.get('Name',''),re.IGNORECASE):
                                    #             self.in_queue_people.put((emp_dets['linkedin_url'],list_items_url_id))
                                else:
                                    logging.info('company part login: res company name is linkedin (default page) for url:{}, thread:{}'.format(url,threading.currentThread()))
                                    no_errors += 1
                            elif 'Notes' in res:
                                if res['Notes'] == 'Not Available Pubicly':
                                    logging.info('company part login: Results not available publicly for url:{}, thread:{}'.format(url,threading.currentThread()))
                                    # self.out_queue.put(res)
                                    no_errors += 1
                                elif res['Notes'] == 'Java script code':
                                    logging.info('company part login: Not proper page, probably javascript for url:{}, thread:{}'.format(url,threading.currentThread()))
                                    # self.out_queue.put(res)
                                    no_errors += 1
                                elif res['Notes'] == 'Company page not found':
                                    logging.info('company part login: Company page not found for url: {} ,thread: {}'.format(url,threading.currentThread()))
                                    self.out_queue_company.put((res,list_items_url_id))
                                    no_errors = 0
                                    n_blocks = 0
                                else:
                                    logging.info('company part login: Notes present, but some unknown error for url:{}, thread:{}'.format(url,threading.currentThread()))
                                    # self.out_queue.put(res)
                                    no_errors += 1
                            else:
                                logging.info('company part login: res has no company name/Notes keys. url:{}, thread:{}'.format(url,threading.currentThread()))
                                no_errors += 1
                        else:
                            logging.info('company part login: res is not present for url:{}, thread:{}'.format(url,threading.currentThread()))
                            no_errors += 1
                    else:
                        logging.info('company part login: res_1 not None, and no result key for url:{}, thread:{}'.format(url,threading.currentThread()))
                        no_errors += 1
                except Exception :
                    logging.exception('company part login: Error while execution for url: {0}, thread: {1}'.format(url,threading.currentThread()))
                    time.sleep(1)
                    no_errors += 1
                if no_errors>0:
                    logging.info('company part login: Something went wrong, '
                                 'could not fetch details for url: {0}, thread id: {1}'.format(url,threading.currentThread()))
                self.in_queue_company.task_done()
            time.sleep(randint(2,7))
            # todo: following part for crawling people pages. but decided not to do this. Instead, write another \
            # program which can take name,company name and designation and search in duckduckgo to find the actual
            # linkedin url and crawl it using the without login option
            # if not self.in_queue_people.empty():
            #     # put a company url if people queue is not empty
            #     url,list_items_url_id = self.in_queue_people.get()
            #     # checking if the url is already crawled
            #     query = "select linkedin_url from {} where list_id = %s and linkedin_url = %s limit 1".format(self.people_base_table)
            #     try:
            #         # self.con.get_cursor()
            #         self.con_check.execute(query,(self.list_id,url,))
            #         url_pres = self.con_check.cursor.fetchall()
            #     except:
            #         url_pres = []
            #     # self.con.close_cursor()
            #     if url_pres:
            #         self.in_queue_people.task_done()
            #         continue
            #     try:
            #         for t_no in range(2):
            #             time.sleep(randint(30,90))
            #             logging.info('people part login: Input URL:{}, thread:{}, try:{}'.format(url,threading.currentThread(),t_no+1))
            #             res_1 = {}
            #             get_output_people(crawler_company,crawler_people,url,res_1)
            #             if res_1.get('result','error_happened') != 'error_happened':
            #                 res = res_1['result']
            #                 if res.get('Name','LinkedIn') != 'LinkedIn' and res.get('Name','LinkedIn') and 'Notes' not in res:
            #                     break
            #         if 'result' in res_1:
            #             res = res_1['result']
            #             if res:
            #                 # pdb.set_trace()
            #                 if 'Name' in res :
            #                     if res['Name'] and ( res['Name'] != 'LinkedIn'):
            #                         self.out_queue_people.put((res,list_items_url_id))
            #                         no_errors = 0
            #                         n_blocks = 0
            #                     else:
            #                         logging.info('people part login: res_1 company name is missing or linkedin (default page),'
            #                                      ' for url:{}, thread:{}'.format(url,threading.currentThread()))
            #                         no_errors += 1
            #                 elif 'Notes' in res:
            #                     if res['Notes'] == 'Not Available Pubicly':
            #                         logging.info('people part login: Not publicly available for url:{}, thread:{}'.format(url,threading.currentThread()))
            #                         # self.out_queue.put(res)
            #                         no_errors += 1
            #                     elif res['Notes'] == 'Java script code':
            #                         logging.info('people part login: Not proper page, probably javascript for url:{}, thread:{}'.format(url,threading.currentThread()))
            #                         # self.out_queue.put(res)
            #                         no_errors += 1
            #                     else:
            #                         logging.info('people part login: Notes present, but some unknown error for url:{}, thread:{}'.format(url,threading.currentThread()))
            #                         # self.out_queue.put(res)
            #                         no_errors += 1
            #                 else:
            #                     logging.info('people part login: res present, but Name or Notes keys not present for url:{}, thread:{}'.format(url,threading.currentThread()))
            #                     no_errors += 1
            #             else:
            #                 logging.info('people part login: res not present for url:{}, thread:{}'.format(url,threading.currentThread()))
            #                 no_errors += 1
            #         else:
            #             logging.info('people part login: res_1 not None, but no result key present for url:{}, thread:{}'.format(url,threading.currentThread()))
            #             no_errors += 1
            #     except Exception :
            #         logging.exception('people part login: Error while execution for url: {0}, Thread: {1}'.format(url,threading.currentThread()))
            #         time.sleep(1)
            #         no_errors += 1
            #     self.in_queue_people.task_done()
            #     if no_errors>0:
            #         logging.info('people part login: Something went wrong, '
            #                      'could not fetch details for url: {0}, thread id: {1}'.format(url,threading.currentThread()))
                
            if ind%10 == 0 :
                time.sleep(randint(2,6))
                if ind%50 == 0 :
                    # logging.info('Completed URLs: {}, Error URLs: {}, URLs to crawl: {}'.format(len(self.processed_queue.queue),
                    #                                                          len(self.error_queue.queue),
                    #                                                          len(self.in_queue.queue)))
                    time.sleep(randint(10,20))
                if ind%100 == 0:
                    time.sleep(randint(50,70))
            if no_errors == 6:
                no_errors = no_errors - 1
                n_blocks += 1
                if self.use_tor:
                    pass
                elif not self.proxy:
                    logging.info('company part login: Error condition met, sleeping for '+str(min(n_blocks,20)*20)+' seconds')
                    time.sleep(min(n_blocks,20)*20)
                else: #if proxy get another proxy
                    # time.sleep(randint(10,20))
                    logging.info('company part login: Error condition met, trying to use another ip. '
                                 'current ip : {}, thread: {}'.format(proxy_dets,threading.currentThread()))
                    try:
                        crawler_company.exit()
                        time.sleep(min(n_blocks,20)*20)
                        try:
                            logging.info('company part login: trying to kill firefox : pid:{}'.format(crawler_company.link_parser.pid))
                            os.system('kill -9 {}'.format(crawler_company.link_parser.pid))
                        except:
                            logging.exception('company part login: Couldnt kill firefox')
                            pass
                        logging.info('company part login: Getting proxy ip details, thread: {0}'.format(threading.currentThread()))
                        proxy_dets = self.get_proxy()
                        logging.info('company part login: proxy to be used: {0}, thread:{1}'.format(proxy_dets,threading.currentThread()))
                        proxy_ip,proxy_port = proxy_dets[0],proxy_dets[1]
                        crawler_company.init_selenium_parser(self.browser,self.visible,proxy=self.proxy,
                                                     proxy_ip=proxy_ip,proxy_port=proxy_port,use_tor=self.use_tor)
                    except:
                        logging.exception('company part login: Exception while trying to change ip, use same parser, thread:{}'.format(threading.currentThread()))
                        try:
                            crawler_company.init_selenium_parser() #try with already existing parameters
                        except:
                            logging.exception('company part login: Exception, can not restart crawler with already existing parameters, trying to restart, thread:{}'.format(threading.currentThread()))
                            try:
                                try:
                                    del crawler_company
                                    gc.collect()
                                except:
                                    pass
                                crawler_company = linkedin_company_crawler_login.LinkedinOrganizationService(self.browser,self.visible,proxy=self.proxy,
                                                                      proxy_ip=proxy_ip,proxy_port=proxy_port,use_tor=self.use_tor
                                                                      )
                                # self.crawler_queue.put(crawler)
                            except:
                                logging.exception('company part login: could not start crawler, thread:{0}'.format(threading.currentThread()))
                                self.run_queue = False # stop crawling if it reaches here
                                # break #stop this thread??
                # self.error_queue.put(url)

        logging.info('company part login: exiting crawler, thread:{}'.format(threading.currentThread()))
        crawler_company.exit()
        logging.info('company part login: crawler exited, thread:{}'.format(threading.currentThread()))

    def save_to_table_company(self,res,list_items_url_id):
        '''
        :param res:
        :return:
        '''
        # self.con.get_cursor()
        # check if there is Notes. If true, remove the url from url to crawl list
        if res.get('Notes',''): #need to insert these into db saying not available
            self.con.cursor.execute("DELETE FROM {} WHERE url = %s and list_id = %s".format(
                self.company_urls_to_crawl_table),(res['Linkedin URL'],self.list_id,))
            if res.get('Linkedin URL','') != res.get('Original URL',''):
                self.con.cursor.execute("DELETE FROM {} WHERE url = %s and list_id = %s".format(
                    self.company_urls_to_crawl_table),(res['Original URL'],self.list_id,))
            self.con.commit()
            return
        # first convert the connected fields into str. Otherwise the insert into table fails
        if res.get('Employee Details',[]):
            employee_urls = [re.sub(linkedin_url_clean_regex,'',com_dic.get('linkedin_url','')) + '{}' +
                             com_dic.get('Name','') + '{}' + com_dic.get('Designation','')+ '{}'+
                             com_dic.get('Industry','') + '{}'+com_dic.get('Location','')+'{}'+
                             com_dic.get('current_company','')
                             for com_dic in res['Employee Details']]
            res['Employee Details'] = '|'.join(employee_urls)
        else:
            # employee_urls = []
            res['Employee Details'] = ''
        if res.get('Also Viewed Companies',[]):
            also_viewed_urls = [re.sub(linkedin_url_clean_regex,'',com_dic.get('company_linkedin_url','')) + '{}' +
                                com_dic.get('Company Name','')
                                for com_dic in res['Also Viewed Companies']]
            res['Also Viewed Companies'] = '|'.join(also_viewed_urls)
        else:
            # also_viewed_urls = []
            res['Also Viewed Companies'] = ''
        res['list_id'] = self.list_id
        res['list_items_url_id'] = list_items_url_id
        res_fields = []
        for field in self.table_fields_company:
            field_val = res.get(field,'NULL')
            try:
                res_fields.append(field_val.strip())
            except:
                res_fields.append('')
        query = "INSERT INTO {} ({}) VALUES ( {} )".format(
            self.company_base_table,','.join(self.table_field_names_company),','.join(['%s']*len(self.table_fields_company)))
        self.con.cursor.execute(query,res_fields)
        self.con.cursor.execute("INSERT INTO {} (url,list_id,list_items_url_id) VALUES (%s,%s,%s) ON "\
                "CONFLICT DO NOTHING".format(self.finished_urls_table_company),(res['Linkedin URL'],self.list_id,list_items_url_id,))
        self.con.cursor.execute("DELETE FROM {} WHERE url = %s and list_id = %s".format(
            self.company_urls_to_crawl_table),(res['Linkedin URL'],self.list_id,))
        if res.get('Linkedin URL','') != res.get('Original URL',''):
            self.con.cursor.execute("INSERT INTO {} (url,list_id,list_items_url_id) VALUES (%s,%s,%s) "\
                    "ON CONFLICT DO NOTHING".format(self.finished_urls_table_company),(res['Original URL'],self.list_id,list_items_url_id,))
            self.con.cursor.execute("DELETE FROM {} WHERE url = %s and list_id = %s".format(
                self.company_urls_to_crawl_table),(res['Original URL'],self.list_id,))
        self.con.cursor.execute("INSERT INTO crawler.linkedin_company_redirect_url (url,redirect_url) VALUES (%s,%s),(%s,%s) ON CONFLICT DO NOTHING",
                                (res['Original URL'],res['Linkedin URL'],res['Linkedin URL'],res['Linkedin URL'],))
        self.con.commit()
        # self.con.close_cursor()
        logging.info('company part login: saved to database for url:{}'.format(res['Linkedin URL']))

    def save_to_table_people(self,res,list_items_url_id):
        '''not used because we are not crawling people pages after logging in
        :param res:
        :return:
        '''
        # self.con.get_cursor()
        if res.get('Related People',[]):
            related_people_urls = [re.sub(linkedin_url_clean_regex,'',com_dic.get('Linkedin Page','')) + '{}' +
                                   com_dic.get('Name','') + '{}' + com_dic.get('Position')
                                   for com_dic in res['Related People']]
            res['Related People'] = '|'.join(related_people_urls)
        else:
            related_people_urls = []
            res['Related People'] = ''
        if res.get('Same Name People',[]):
            same_name_urls = [re.sub(linkedin_url_clean_regex,'',com_dic.get('Linkedin Page','')) + '{}' +
                              com_dic.get('Name','') + '{}' + com_dic.get('Position','')
                              for com_dic in res['Same Name People']]
            res['Same Name People'] = '|'.join(same_name_urls)
        else:
            same_name_urls = []
            res['Same Name People'] = ''
        if res.get('Experience',[]):
            company_urls = [re.sub(linkedin_url_clean_regex,'',com_dic.get('Company Linkedin','')) + '{}'+
                            com_dic.get('Position','') + '{}' + com_dic.get('Company','') + '{}' +
                            com_dic.get('Date Range','') + '{}' + com_dic.get('Location','') + '{}' +
                            com_dic.get('Description','')
                            for com_dic in res['Experience']]
            res['Experience'] = '|'.join(company_urls)
        else:
            company_urls = []
            res['Experience'] = ''
        res['CompanyLinkedinPage'] = re.sub(linkedin_url_clean_regex,'',res.get('CompanyLinkedinPage',''))
        res['list_id'] = self.list_id
        res['list_items_url_id'] = list_items_url_id
        res_fields = []
        for field in self.table_fields_people:
            field_val = res.get(field,'NULL')
            try:
                res_fields.append(field_val.strip())
            except:
                res_fields.append('')
        query = "INSERT INTO {} ({}) VALUES ( {} )".format(
            self.people_base_table,','.join(self.table_field_names_people),','.join(['%s']*len(self.table_fields_people)))
        # logging.info('query: {} ,insert row: {}'.format(query,res_fields))
        self.con.cursor.execute(query,res_fields)
        # add the url to finished url
        self.con.cursor.execute("INSERT INTO {} (url,list_id,list_items_url_id) VALUES (%s,%s,%s) ON "\
                "CONFLICT DO NOTHING".format(self.finished_urls_table_people),(res['Linkedin URL'],self.list_id,list_items_url_id,))
        self.con.cursor.execute("DELETE FROM {} WHERE url = %s and list_id = %s".format(
            self.people_urls_to_crawl_table),(res['Linkedin URL'],self.list_id,))
        # if res.get('Linkedin URL','') != res.get('Original URL',''):
        #     self.con.cursor.execute("INSERT INTO {} (url,list_id,list_items_url_id) VALUES (%s,%s,%s) "\
        #             "ON CONFLICT DO NOTHING".format(self.finished_urls_table_people),(res['Original URL'],self.list_id,list_items_url_id,))
        #     self.con.cursor.execute("DELETE FROM {} WHERE url = %s and list_id = %s".format(self.people_urls_to_crawl_table),(res['Original URL'],self.list_id,))
        self.con.cursor.execute("INSERT INTO crawler.linkedin_people_redirect_url (url,redirect_url) VALUES (%s,%s),(%s,%s) ON CONFLICT DO NOTHING",
                               (res['Original URL'],res['Linkedin URL'],res['Linkedin URL'],res['Linkedin URL'],))
        self.con.commit()
        #self.con.close_cursor()
        logging.info('people part: saved to database for url:{}'.format(res['Linkedin URL']))


    def worker_save_res(self):
        '''
        :return:
        '''
        while self.run_write_queue:
            if not self.out_queue_company.empty():
                res,list_items_url_id = self.out_queue_company.get()
                if not list_items_url_id:
                    logging.info('company part login: list_items_url_id is not present for res: {}'.format(res))
                    continue
                self.save_to_table_company(res,list_items_url_id)
                self.out_queue_company.task_done()
            # if not self.out_queue_people.empty():
            #     res,list_items_url_id = self.out_queue_people.get()
            #     if not list_items_url_id:
            #         logging.info('people part: list_items_url_id is not present for res: {}'.format(res))
            #         continue
            #     self.save_to_table_people(res,list_items_url_id)
            #     self.out_queue_people.task_done()

    def run(self,n_threads=1,limit_no=2000,
            company_urls_to_crawl_table='crawler.linkedin_company_urls_to_crawl_priority',
            company_base_table='crawler.linkedin_company_base',
            people_base_table='crawler.linkedin_people_base',
            people_urls_to_crawl_table = 'crawler.linkedin_people_urls_to_crawl_priority',
            finished_urls_table_company = 'crawler.linkedin_company_finished_urls',
            finished_urls_table_people = 'crawler.linkedin_people_finished_urls',
            list_id = None,desig_list = [],no_pages_to_search=2):
        '''
        :param n_threads:
        :param limit_no:
        :param company_urls_to_crawl_table:
        :param company_base_table:
        :param people_base_table:
        :param people_urls_to_crawl_table:
        :param finished_urls_table_company:
        :param finished_urls_table_people:
        :param list_id:
        :param desig_list:
        :param no_pages_to_search: while searching in linkedin for people from a company, how many pages to search
        :return:
        '''
        # logging.basicConfig(filename=log_file_loc, level=logging.INFO,format='%(asctime)s %(message)s')
        # if self.use_db:
        self.company_base_table = company_base_table
        self.company_base_table_orig = 'crawler.linkedin_company_base'
        self.people_base_table = people_base_table
        self.company_urls_to_crawl_table = company_urls_to_crawl_table
        self.people_urls_to_crawl_table = people_urls_to_crawl_table
        self.finished_urls_table_company = finished_urls_table_company
        self.finished_urls_table_people = finished_urls_table_people
        self.list_id = list_id
        self.desig_list = desig_list
        self.no_pages_to_search = no_pages_to_search
        # self.crawler_queue = Queue(maxsize=0)
        # self.con.get_cursor() #no need to get cursor separately
        # first look at priority table
        query = "select a.url,a.list_items_url_id from {} a left join {} b on a.url = b.url and a.list_id = b.list_id "\
                "where a.list_id = %s and b.url is NULL limit {}".format(self.company_urls_to_crawl_table,
                                                                         self.finished_urls_table_company,limit_no)
        self.con.cursor.execute(query,(self.list_id,))
        inp_list_priority = self.con.cursor.fetchall()
        # take all company urls with employee details null in base w/o login table and not present in base login table
        query = " select a.url,a.id from {list_items_urls} a join {redirect_table} b on (a.url=b.url or a.url=b.redirect_url) " \
                " join {comp_base_orgi} c on a.list_id=c.list_id and (b.redirect_url=c.linkedin_url or a.url=c.linkedin_url) " \
                " left join {comp_base_login} d on a.list_id=d.list_id and (b.redirect_url=d.linkedin_url or a.url=d.linkedin_url) "\
                " where a.list_id= %s and c.employee_details = '' " \
                " and d.linkedin_url is null limit {limit_no} ".format(list_items_urls='crawler.list_items_urls',
                                                                redirect_table='crawler.linkedin_company_redirect_url',
                                                                comp_base_orgi=self.company_base_table_orig,
                                                                comp_base_login=self.company_base_table,
                                                                limit_no=limit_no)
        self.con.cursor.execute(query,(self.list_id,))
        inp_list_priority.extend(self.con.cursor.fetchall())
        inp_list = inp_list_priority
        inp_list = [(i[0],i[1]) for i in inp_list]
        inp_list = list(set(inp_list))
        if not inp_list:
            logging.info('company part login: no company urls to crawl')
            return
        logging.info('company part login: No of urls for which crawling to be done : {}'.format(len(inp_list)))
        self.run_queue = True
        self.run_write_queue = True
        self.in_queue_company = Queue(maxsize=0)
        self.out_queue_company = Queue(maxsize=0)
        # self.in_queue_people = Queue(maxsize=0)
        # self.out_queue_people = Queue(maxsize=0)
        # self.processed_queue = Queue(maxsize=0)
        # self.error_queue = Queue(maxsize=0)
        self.gen_proxies()
        logging.info('company part login: Starting indivudual threads. No of threads : {}'.format(n_threads))
        for i in range(n_threads):
            worker = threading.Thread(target=self.worker_fetch_url)
            worker.setDaemon(True)
            worker.start()
        worker = threading.Thread(target=self.worker_save_res)
        worker.setDaemon(True)
        worker.start()
        logging.info('company part login: Putting urls to queue')
        for i in inp_list:
            self.in_queue_company.put(i)
        del inp_list
        # while (not self.in_queue_company.empty() or not self.in_queue_people.empty()) and self.run_queue:
        while not self.in_queue_company.empty()  and self.run_queue:
            try:
                time.sleep(2)
            except :
                # self.run_queue = False
                break
        self.run_queue = False
        time.sleep(60)
        logging.info('company part login: crawling stopped, trying to save already crawled results. '
                     'No of results left in out queue : {}'.format(len(self.out_queue_company.queue)))
        if not self.out_queue_company.empty():
            self.out_queue_company.join()
        # if not self.out_queue_people.empty():
        #     self.out_queue_people.join()
        self.run_write_queue = False
        # self.proxy_generator.exit()
        # while not self.crawler_queue.empty():
        #     try:
        #         crl = self.crawler_queue.get()
        #         try:
        #             crl.exit()
        #         except:
        #             pass
        #         del crl
        #     except:
        #         continue
        logging.info('Finished company part login. No of results left in out queue : {}'.format(len(self.out_queue_company.queue)))
        # logging._removeHandlerRef()
        time.sleep(5)
        self.con.close_cursor()
        self.con.close_connection()
        self.con_check.close_cursor()
        self.con_check.close_connection()
        return
