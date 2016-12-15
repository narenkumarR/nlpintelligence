__author__ = 'joswin'
import re
import gc
from Queue import Queue
import threading
import logging
import time
import os
from random import randint

import linkedin_profile_crawler
from proxy_generator import ProxyGen
from postgres_connect import PostgresConnect

linkedin_url_clean_regex=r'\?trk=pub-pbmap|\?trk=prof-samename-picture|\?trk=extra_biz_viewers_viewed|\?trk=biz_employee_pub|\?trk=ppro.cprof'

class LinkedinProfileCrawlerThread(object):
    def __init__(self,browser='Firefox',visible=True,proxy=False,use_tor=False,use_db=False,login=False):
        '''
        :param browser:
        :param visible:
        :param proxy:
        :param use_tor: if use_tor is True, proxy should be False
        :return:
        '''
        self.browser = browser
        self.visible = visible
        if browser == 'Firefox_luminati':
            self.proxy = False
        else:
            self.proxy = proxy
        self.ip_matcher = re.compile("^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")
        self.use_tor = use_tor
        self.use_db = use_db
        self.login = login
        if use_db:
            self.con = PostgresConnect()
            self.con_check = PostgresConnect()
            self.table_fields = ['Linkedin URL','Name','Position','Location','Company','CompanyLinkedinPage',
                                 'PreviousCompanies','Education','Industry','Summary','Skills','Experience'
                                 ,'Related People','Same Name People','list_id','list_items_url_id']
            self.table_field_names = ['linkedin_url','name','sub_text','location','company_name','company_linkedin_url',
                                      'previous_companies','education','industry','summary','skills','experience',
                                      'related_people','same_name_people','list_id','list_items_url_id']
        if self.proxy:
            self.proxy_generator = ProxyGen(browser_name=browser,visible=visible,page_load_timeout=60)
        self.proxies = Queue(maxsize=0)
        self.sleep_time_min = 60
        self.sleep_time_max = 120
        if self.browser == 'Firefox_luminati':
            self.error_limit = 2
        else:
            self.error_limit = 3

    def gen_proxies(self):
        '''
        :return:
        '''
        logging.info('people part: trying to get proxies')
        if not self.proxy :
            return [(None,None)]
        else:
            try:
                self.proxy_generator.activate_browser()
                proxies = self.proxy_generator.generate_proxy()
            except Exception :
                logging.exception('People part: could not create proxies. using None')
                proxies = [(None,None)]
            try:
                self.proxy_generator.exit()
            except:
                pass
        logging.info('people part: Proxies fetched {}'.format(proxies))
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
        try: #some error happens while getting proxy sometimes. putting it in try
            logging.info('People part: No proxies in queue,trying to get proxies')
            proxy_dets = self.get_proxy()
            logging.info('People part: proxy to be used : {}'.format(proxy_dets))
            proxy_ip,proxy_port = proxy_dets[0],proxy_dets[1]
            crawler = linkedin_profile_crawler.LinkedinProfileCrawler(self.browser,self.visible,proxy=self.proxy,
                                                                      proxy_ip=proxy_ip,proxy_port=proxy_port,
                                                                      use_tor=self.use_tor,login=self.login)
        except: #if  coming here, run without proxy
            logging.info('People part: Error while getting proxy. Running without proxy')
            proxy_ip,proxy_port = None,None
            crawler = linkedin_profile_crawler.LinkedinProfileCrawler(self.browser,self.visible,proxy=False,
                                                                      proxy_ip=proxy_ip,proxy_port=proxy_port,
                                                                      use_tor=self.use_tor,login=self.login)
        def get_output(crawler,url,res_1,event):
            res_1['result'] = crawler.fetch_details_urlinput(url)
            event.set()
        no_errors = 0
        ind = 0
        n_blocks =0
        # self.crawler_queue.put(crawler)
        while self.run_queue:
            ind += 1
            url,list_items_url_id = self.in_queue.get()
            # checking if the url is already crawled
            query = "select linkedin_url from {} where list_id = %s and linkedin_url = %s limit 1".format(self.base_table)
            try:
                # self.con.get_cursor()
                self.con_check.execute(query,(self.list_id,url,))
                url_pres = self.con_check.cursor.fetchall()
            except:
                url_pres = []
            # self.con.close_cursor()
            if url_pres:
                continue
            try:
                time.sleep(randint(self.sleep_time_min,self.sleep_time_max))
                if self.browser == 'Firefox_luminati':
                    soup = crawler.link_parser.get_soup('https://lumtest.com/myip.json')
                    ip_dets = soup.find('pre').text
                    logging.info('People crawler, luminati browser info: user agent: {usr_agent}, ip: {ip_dets}'.format(
                        usr_agent = crawler.link_parser.user_agent,ip_dets=ip_dets.decode('ascii','ignore')
                    ))
                for t_no in range(2):
                    # time.sleep(randint(30,90))
                    logging.info('people part: Input URL:{}, thread:{}, try:{}, no_errors: {}'.format(url,threading.currentThread(),t_no+1,no_errors))
                    res_1 = {}
                    event = threading.Event()
                    t1 = threading.Thread(target=get_output, args=(crawler,url,res_1,event,))
                    t1.daemon = True
                    t1.start()
                    # event.wait(timeout=(60+t_no*60)) #first iteration wait for 60 secs. next time 120
                    event.wait(timeout=120)
                    if res_1.get('result','error_happened') != 'error_happened':
                        res = res_1['result']
                        if res.get('Name','LinkedIn') != 'LinkedIn' and res.get('Name','LinkedIn') and 'Notes' not in res:
                            break
                if res_1 is None: #if None means timeout happened, push to queue again
                    logging.info('People part: res_1 None, probably timeout, for url:{}, thread:{}'.format(url,threading.currentThread()))
                    # self.in_queue.put(url)
                    no_errors += 1
                elif 'result' in res_1:
                    res = res_1['result']
                    if res:
                        if 'Name' in res :
                            if res['Name'] and (not self.login and res['Name'] != 'LinkedIn'):
                                self.out_queue.put((res,list_items_url_id))
                                no_errors = 0
                                n_blocks = 0
                            else:
                                logging.info('People part: res_1 company name is linkedin (default page) for url:{}, thread:{}'.format(url,threading.currentThread()))
                                no_errors += 1
                        elif 'Notes' in res:
                            if res['Notes'] == 'Not Available Pubicly':
                                logging.info('People part: Not publicly available for url:{}, thread:{}'.format(url,threading.currentThread()))
                                # self.out_queue.put(res)
                                no_errors += 1
                            elif res['Notes'] == 'Java script code':
                                logging.info('People part: Not proper page, probably javascript for url:{}, thread:{}'.format(url,threading.currentThread()))
                                # self.out_queue.put(res)
                                no_errors += 1
                            else:
                                logging.info('People part: Notes present, but some unknown error for url:{}, thread:{}'.format(url,threading.currentThread()))
                                # self.out_queue.put(res)
                                no_errors += 1
                        else:
                            logging.info('People part: res present, but Name or Notes keys not present for url:{}, thread:{}'.format(url,threading.currentThread()))
                            no_errors += 1
                    else:
                        logging.info('People part: res not present for url:{}, thread:{}'.format(url,threading.currentThread()))
                        no_errors += 1
                else:
                    logging.info('People part: res_1 not None, but no result key present for url:{}, thread:{}'.format(url,threading.currentThread()))
                    no_errors += 1
            except Exception :
                logging.exception('People part: Error while execution for url: {0}, Thread: {1}'.format(url,threading.currentThread()))
                time.sleep(1)
                no_errors += 1
            if ind%10 == 0:
                time.sleep(randint(2,6))
                if ind%100 == 0:
                    time.sleep(randint(10,20))
                if ind%500 == 0:
                    time.sleep(randint(50,70))
            if no_errors >= self.error_limit:
                no_errors = self.error_limit
                no_errors = no_errors - 1
                if self.use_tor:
                    pass
                elif not self.proxy and self.browser != 'Firefox_luminati':
                    n_blocks += 1
                    logging.info('People part: Error condition met, sleeping for '+str(min(n_blocks,6)*600)+' seconds')
                    time.sleep(min(n_blocks,6)*60)
                else:
                    time.sleep(randint(10,20))
                    logging.info('People part: Error condition met, trying to use another ip. Thread: {0}, Current ip: {1}'.format(threading.currentThread(), proxy_dets))
                    try:
                        crawler.exit()
                        try:
                            logging.info('people part: trying to kill firefox : pid:{}'.format(crawler.link_parser.pid))
                            os.system('kill -9 {}'.format(crawler.link_parser.pid))
                        except:
                            logging.exception('people part: Couldnt kill firefox')
                            pass
                        logging.info('people part: Getting proxy ip details, thread: {0}'.format(threading.currentThread()))
                        proxy_dets = self.get_proxy()
                        logging.info('People part: proxy to be used: {0}, Thread:{1}'.format(proxy_dets,threading.currentThread()))
                        proxy_ip,proxy_port = proxy_dets[0],proxy_dets[1]
                        crawler.init_selenium_parser(self.browser,self.visible,proxy=self.proxy,
                                                            proxy_ip=proxy_ip,proxy_port=proxy_port,use_tor=self.use_tor)
                    except:
                        logging.exception('People part: Exception while trying to change ip, use same parser, thread:{}'.format(threading.currentThread()))
                        try:
                            crawler.init_selenium_parser() #try with already existing parameters
                        except:
                            logging.exception('People part: Exception, can not restart crawler with already existing parameters, trying to restart, thread:{}'.format(threading.currentThread()))
                            try:
                                try:
                                    del crawler
                                    gc.collect()
                                except:
                                    pass
                                crawler = linkedin_profile_crawler.LinkedinProfileCrawler(self.browser,self.visible,proxy=self.proxy,
                                                                      proxy_ip=proxy_ip,proxy_port=proxy_port,use_tor=self.use_tor,
                                                                      login=self.login)
                                # self.crawler_queue.put(crawler)
                            except:
                                logging.exception('People part: could not start crawler')
                                self.run_queue = False # stop crawling if it reaches here
            # if no_errors>0:
            #     logging.info('People part: Something went wrong, could not fetch details for url: {}, thread id: {}'.format(url,threading.currentThread()))
            self.in_queue.task_done()
        logging.info('People part: exiting crawler, thread:{}'.format(threading.currentThread()))
        crawler.exit()
        time.sleep(10)
        logging.info('People part: crawler exited, thread:{}'.format(threading.currentThread()))

    def save_to_table(self,res,list_items_url_id):
        '''
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
        for field in self.table_fields:
            field_val = res.get(field,'NULL')
            try:
                res_fields.append(field_val.strip())
            except:
                res_fields.append('')
        query = "INSERT INTO {} ({}) VALUES ( {} )".format(self.base_table,','.join(self.table_field_names),','.join(['%s']*len(self.table_fields)))
        # logging.info('query: {} ,insert row: {}'.format(query,res_fields))
        self.con.cursor.execute(query,res_fields)
        # add the url to finished url
        self.con.cursor.execute("INSERT INTO {} (url,list_id,list_items_url_id) VALUES (%s,%s,%s) ON "\
                "CONFLICT DO NOTHING".format(self.finished_urls_table_people),(res['Linkedin URL'],self.list_id,list_items_url_id,))
        self.con.cursor.execute("DELETE FROM {} WHERE url = %s and list_id = %s".format(self.urls_to_crawl_table),(res['Linkedin URL'],self.list_id,))
        self.con.cursor.execute("DELETE FROM {} WHERE url = %s and list_id = %s".format(self.urls_to_crawl_priority),(res['Linkedin URL'],self.list_id,))
        if res.get('Linkedin URL','') != res.get('Original URL',''):
            self.con.cursor.execute("INSERT INTO {} (url,list_id,list_items_url_id) VALUES (%s,%s,%s) "\
                    "ON CONFLICT DO NOTHING".format(self.finished_urls_table_people),(res['Original URL'],self.list_id,list_items_url_id,))
            self.con.cursor.execute("DELETE FROM {} WHERE url = %s and list_id = %s".format(self.urls_to_crawl_table),(res['Original URL'],self.list_id,))
            self.con.cursor.execute("DELETE FROM {} WHERE url = %s and list_id = %s".format(self.urls_to_crawl_priority),(res['Original URL'],self.list_id,))
        self.con.cursor.execute("INSERT INTO crawler.linkedin_people_redirect_url (url,redirect_url) VALUES (%s,%s),(%s,%s) ON CONFLICT DO NOTHING",
                               (res['Original URL'],res['Linkedin URL'],res['Linkedin URL'],res['Linkedin URL'],))
        # following part of updating urls to crawl tables is not needed as it is taken care by table updation script
        # if related_people_urls or same_name_urls:
        #     if related_people_urls:
        #         related_people_urls = [i.split('{}')[0] for i in related_people_urls]
        #     if same_name_urls:
        #         same_name_urls = [i.split('{}')[0] for i in same_name_urls]
        #     people_urls = list(set(related_people_urls+same_name_urls))
        #     people_urls = [i for i in people_urls if re.search(r'linkedin',i)]
        #     if people_urls:
        #         self.con.cursor.execute(self.con.cursor.mogrify("SELECT url FROM {} WHERE "\
        #             "list_id = %s and url IN %s".format(self.finished_urls_table_people), (self.list_id,tuple(people_urls),)))
        #         already_crawled_urls = self.con.cursor.fetchall()
        #         already_crawled_urls = [i[0] for i in already_crawled_urls]
        #         urls_to_crawl = list(set(people_urls)-set(already_crawled_urls))
        #         if urls_to_crawl:
        #             records_list_template = ','.join(['%s']*len(urls_to_crawl))
        #             insert_query = "INSERT INTO {} (url,list_id,list_items_url_id) VALUES {} "\
        #                     "ON CONFLICT DO NOTHING".format(self.urls_to_crawl_table,records_list_template)
        #             urls_to_crawl1 = [(i,self.list_id,list_items_url_id,) for i in urls_to_crawl]
        #             self.con.cursor.execute(insert_query, urls_to_crawl1)
        #
        # if company_urls:
        #     company_urls = [i.split('{}')[0] for i in company_urls]
        #     company_urls = [i for i in company_urls if re.search(r'linkedin',i)]
        #     if company_urls:
        #         self.con.cursor.execute(self.con.cursor.mogrify("SELECT url FROM {} WHERE "\
        #             "list_id = %s and url IN %s".format(self.finished_urls_table_people), (self.list_id,tuple(company_urls),)))
        #         already_crawled_urls = self.con.cursor.fetchall()
        #         already_crawled_urls = [i[0] for i in already_crawled_urls]
        #         urls_to_crawl = list(set(company_urls)-set(already_crawled_urls))
        #         if urls_to_crawl:
        #             records_list_template = ','.join(['%s']*len(urls_to_crawl))
        #             insert_query = "INSERT INTO {} (url,list_id,list_items_url_id) VALUES {} "\
        #                     "ON CONFLICT DO NOTHING".format(self.urls_to_crawl_table,records_list_template)
        #             urls_to_crawl1 = [(i,self.list_id,list_items_url_id,) for i in urls_to_crawl]
        #             self.con.cursor.execute(insert_query, urls_to_crawl1)
        self.con.commit()
        #self.con.close_cursor()
        logging.info('people part: saved to database for url:{}'.format(res['Linkedin URL']))


    def worker_save_res(self):
        '''
        :return:
        '''
        while self.run_write_queue:
            res,list_items_url_id = self.out_queue.get()
            if not list_items_url_id:
                logging.info('people part: list_items_url_id is not present for res: {}'.format(res))
                continue
            if self.use_db:
                self.save_to_table(res,list_items_url_id)
            else:
                if res is None:
                    break
                with open(self.out_loc,'a') as f:
                    f.write(str(res)+'\n')
            self.out_queue.task_done()

    def run(self,inp_list,out_loc,log_file_loc,n_threads=2,limit_no=2000,urls_to_crawl_table='crawler.linkedin_people_urls_to_crawl',
            urls_to_crawl_priority='crawler.linkedin_people_urls_to_crawl_priority',base_table='crawler.linkedin_people_base',
            urls_to_crawl_table_company = 'crawler.linkedin_company_urls_to_crawl',
            finished_urls_table_company = 'crawler.linkedin_company_finished_urls',
            finished_urls_table_people = 'crawler.linkedin_people_finished_urls',
            list_id = None):
        '''
        :param inp_list:
        :param out_loc:
        :return:
        '''
        # logging.basicConfig(filename=log_file_loc, level=logging.INFO,format='%(asctime)s %(message)s')
        if self.use_db:
            self.urls_to_crawl_table = urls_to_crawl_table
            self.urls_to_crawl_priority = urls_to_crawl_priority
            self.base_table = base_table
            self.urls_to_crawl_table_company = urls_to_crawl_table_company
            self.finished_urls_table_company = finished_urls_table_company
            self.finished_urls_table_people = finished_urls_table_people
            self.list_id = list_id
            # self.crawler_queue = Queue(maxsize=0)
            # self.con.get_cursor() #no need to get cursor separately
            # first look at priority table
            query = "select a.url,a.list_items_url_id from {} a left join {} b on a.url = b.url and a.list_id = b.list_id "\
                    "where a.list_id = %s and b.url is NULL order by random() ".format(self.urls_to_crawl_priority,
                                                                             self.finished_urls_table_people)
            self.con.cursor.execute(query,(self.list_id,))
            inp_list_priority = self.con.cursor.fetchall()
            limit_no = limit_no - len(inp_list_priority)
            # deleting the urls from priority table and inserting to base urls_to_crawl table
            # if inp_list_priority:
            #     records_list_template = ','.join(['%s']*len(inp_list_priority))
            #     query = "DELETE FROM {} WHERE url in ({}) and list_id = %s ".format(
            #         self.urls_to_crawl_priority,records_list_template,self.list_id)
            #     self.con.cursor.execute(query, [i[0] for i in inp_list_priority]+[self.list_id])
            #     # following query is to make sure this url is not lost when a url in priority table is lost for some reason
            #     query = "INSERT INTO {} (url,list_id,list_items_url_id) VALUES {} ON CONFLICT DO NOTHING".format(self.urls_to_crawl_table,
            #                                                                      records_list_template)
            #     self.con.cursor.execute(query, [(i[0],self.list_id,i[1]) for i in inp_list_priority])
            #     self.con.commit()
            if limit_no>0:
                query = "select url,list_items_url_id from {} where list_id = %s "\
                        " offset floor(random() * (select count(*) from {} where list_id = %s)) "\
                        "limit {}".format(self.urls_to_crawl_table,self.urls_to_crawl_table,limit_no)
                self.con.cursor.execute(query,(self.list_id,self.list_id,))
                inp_list = self.con.cursor.fetchall()
            else:
                inp_list = []
            # creating final list
            inp_list = inp_list_priority+inp_list
            inp_list = [(i[0],i[1]) for i in inp_list]
            inp_list = list(set(inp_list))

        if not inp_list:
            logging.info('People part: no people urls to crawl')
            return
        logging.info('People part: No of  urls for which crawling to be done : {}'.format(len(inp_list)))
        self.run_queue = True
        self.run_write_queue = True
        self.out_loc = out_loc
        self.in_queue = Queue(maxsize=0)
        self.out_queue = Queue(maxsize=0)
        # self.processed_queue = Queue(maxsize=0)
        self.gen_proxies() # generating proxies first. If this is not done, all threads will independantly try to generate
        logging.info('People part: starting threads, no of threads: {}'.format(n_threads))
        for i in range(n_threads):
            worker = threading.Thread(target=self.worker_fetch_url)
            worker.setDaemon(True)
            worker.start()
            time.sleep(10)
        worker = threading.Thread(target=self.worker_save_res)
        worker.setDaemon(True)
        worker.start()
        logging.info('People part: putting urls into queue')
        for i in inp_list:
            self.in_queue.put(i)
        # self.in_queue.join()
        #doing while loop instead of join to save all the results in the queue. not sure if this is working or not.
        del inp_list
        while not self.in_queue.empty() and self.run_queue:
            try:
                time.sleep(100)
            except :
                # self.run_queue = False
                break
        self.run_queue = False
        time.sleep(self.sleep_time_max+240) #giving 20 second wait for all existing tasks to finish
        logging.info('people part: crawling stopped, trying to save already crawled results. No of results left in out queue : {}'.format(len(self.out_queue.queue)))
        if not self.out_queue.empty():
            self.out_queue.join()
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
        logging.info('Finished people part. No of results left in out queue : {}'.format(len(self.out_queue.queue)))
        # logging._removeHandlerRef()
        time.sleep(5)
        if self.use_db:
            self.con.close_cursor()
            self.con.close_connection()
            self.con_check.close_cursor()
            self.con_check.close_connection()
        return

