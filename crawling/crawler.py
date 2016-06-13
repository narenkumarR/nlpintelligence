__author__ = 'joswin'

import re
from Queue import Queue
import threading
import logging
import time
from random import randint

import linkedin_company_crawler,linkedin_profile_crawler
from proxy_generator import ProxyGen
from postgres_connect import PostgresConnect

linkedin_url_clean_regex=r'\?trk=pub-pbmap|\?trk=prof-samename-picture|\?trk=extra_biz_viewers_viewed|\?trk=biz_employee_pub|\?trk=ppro.cprof'

class LinkedinCompanyCrawlerThread(object):
    def __init__(self,browser='Firefox',visible=True,proxy=False,use_tor=False,use_db=False):
        '''
        :param browser:
        :param visible:
        :param proxy:
        :param use_tor:
        :param use_db:
        :return:
        '''
        self.browser = browser
        self.visible = visible
        self.proxy = proxy
        self.ip_matcher = re.compile("^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")
        self.use_tor = use_tor
        self.use_db = use_db
        if use_db:
            self.con = PostgresConnect()
            self.table_fields = ['Linkedin URL','Company Name','Company Size','Industry','Type','Headquarters',
                                 'Description Text','Founded','Specialties','Website'
                ,'Employee Details','Also Viewed Companies']
            self.table_field_names = ['linkedin_url','company_name','company_size','industry','company_type','headquarters',
                                      'description','founded','specialties','website','employee_details','also_viewed_companies']
        if proxy:
            self.proxy_generator = ProxyGen(visible=visible,page_load_timeout=25)
            self.proxies = Queue(maxsize=0)
            # self.proxies.put((None,None)) #try with actual ip first time
            # self.gen_proxies() # logging problem if this runs before init. put this in run call

    def gen_proxies(self):
        '''
        :return:
        '''
        if not self.proxy:
            return [(None,None)]
        else:
            try:
                proxies = self.proxy_generator.generate_proxy()
            except Exception :
                logging.exception('could not create proxies. using None')
                proxies = [(None,None)]
        logging.info('Proxies fetched {}'.format(proxies))
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
        return self.proxies.get()

    def worker_fetch_url(self):
        '''
        :return:
        '''
        try: #some error happens while getting proxy sometimes. putting it in try
            proxy_dets = self.get_proxy()
            logging.info('proxy to be used : {}'.format(proxy_dets))
            proxy_ip,proxy_port = proxy_dets[0],proxy_dets[1]
            crawler = linkedin_company_crawler.LinkedinOrganizationService(self.browser,self.visible,proxy=self.proxy,
                                                                      proxy_ip=proxy_ip,proxy_port=proxy_port,use_tor=self.use_tor)
        except: #if  coming here, run without proxy
            logging.exception('Error while getting proxy. Running without proxy ')
            proxy_ip, proxy_port = None,None
            crawler = linkedin_company_crawler.LinkedinOrganizationService(self.browser,self.visible,proxy=False,
                                                                      proxy_ip=proxy_ip,proxy_port=proxy_port,use_tor=self.use_tor)
        def get_output(crawler,url,res_1,event):
            res_1['result'] = crawler.get_organization_details_from_linkedin_link(url)
            event.set()
        no_errors = 0
        ind = 0
        n_blocks =0
        while self.run_queue:
            ind += 1
            url = self.in_queue.get()
            # if url in self.processed_queue.queue or url in self.error_queue.queue:
            #     continue
            logging.info('Input URL:{}, thread:{}'.format(url,threading.currentThread()))
            try:
                time.sleep(randint(1,2))
                res_1 = {}
                event = threading.Event()
                t1 = threading.Thread(target=get_output, args=(crawler,url,res_1,event,))
                t1.daemon = True
                t1.start()
                event.wait(timeout=30)
                if res_1 is None: #if None means timeout happened, push to queue again
                    crawler.exit()
                    self.in_queue.put(url)
                    # no_errors += 1
                elif 'result' in res_1:
                    res = res_1['result']
                    if res:
                        if 'Company Name' in res :
                            if res['Company Name'] and res['Company Name'] != 'LinkedIn':
                                self.out_queue.put(res)
                                # self.processed_queue.put(url)
                                no_errors = 0
                                n_blocks = 0
                                # if 'Also Viewed Companies' in res:
                                    # for com_dic in res['Also Viewed Companies']:
                                    #     if com_dic['company_linkedin_url'] not in self.processed_queue.queue:
                                    #         self.in_queue.put(com_dic['company_linkedin_url'])
                                # if res['Company Name'] not in self.processed_queue.queue:
                                #     self.processed_queue.put(res['Company Name'])
                                #     self.out_queue.put(res)
                                #     no_errors = 0
                                #     n_blocks = 0
                                # else:
                                #     logging.info('Duplicate name while processing url:'+url+'. Duplicate value:'+res['Company Name'])
                                #     no_errors += 1
                            else:
                                no_errors += 1
                        elif 'Notes' in res:
                            if res['Notes'] == 'Not Available Pubicly':
                                self.out_queue.put(res)
                            elif res['Notes'] == 'Java script code':
                                self.out_queue.put(res)
                        else:
                            no_errors += 1
                    else:
                        no_errors += 1
                else:
                    no_errors += 1
            except Exception :
                logging.exception('Error while execution for url: {0}, thread: {1}'.format(url,threading.currentThread()))
                time.sleep(1)
                no_errors += 1
            if ind%10 == 0:
                time.sleep(randint(2,6))
                if ind%100 == 0:
                    # logging.info('Completed URLs: {}, Error URLs: {}, URLs to crawl: {}'.format(len(self.processed_queue.queue),
                    #                                                          len(self.error_queue.queue),
                    #                                                          len(self.in_queue.queue)))
                    time.sleep(randint(10,20))
            if no_errors == 6:
                no_errors = no_errors - 1
                if self.use_tor:
                    pass
                elif not self.proxy:
                    n_blocks += 1
                    logging.info('Error condition met, sleeping for '+str(min(n_blocks,6)*600)+' seconds')
                    time.sleep(min(n_blocks,6)*600)
                else: #if proxy get another proxy
                    logging.info('Error condition met, trying to use another ip. current ip : {}, thread: {}'.format(proxy_dets,threading.currentThread()))
                    try:
                        crawler.exit()
                        logging.info('Getting proxy ip details, thread: {0}'.format(threading.currentThread()))
                        proxy_dets = self.get_proxy()
                        logging.info('proxy to be used: {0}, thread:{1}'.format(proxy_dets,threading.currentThread()))
                        proxy_ip,proxy_port = proxy_dets[0],proxy_dets[1]
                        crawler.init_selenium_parser(self.browser,self.visible,proxy=self.proxy,
                                                                  proxy_ip=proxy_ip,proxy_port=proxy_port,use_tor=self.use_tor)
                    except:
                        logging.exception('Exception while trying to change ip, use same parser, thread:{}'.format(threading.currentThread()))
                        try:
                            crawler.init_selenium_parser() #try with already existing parameters
                        except:
                            logging.exception('Exception, can not restart crawler with already existing parameters, trying to restart, thread:{}'.format(threading.currentThread()))
                            try:
                                crawler = linkedin_company_crawler.LinkedinOrganizationService(self.browser,self.visible,proxy=self.proxy,
                                                                      proxy_ip=proxy_ip,proxy_port=proxy_port,use_tor=self.use_tor)
                            except:
                                logging.exception('could not start crawler, thread:{0}'.format(threading.currentThread()))
                                # break #stop this thread??
            if no_errors>0:
                logging.info('Something went wrong, could not fetch details for url: {0}, thread id: {1}'.format(url,threading.currentThread()))
                # self.error_queue.put(url)
            self.in_queue.task_done()
        logging.info('exiting crawler, thread:{}'.format(threading.currentThread()))
        crawler.exit()
        logging.info('crawler exited, thread:{}'.format(threading.currentThread()))

    def save_to_table(self,res):
        '''
        :param res:
        :return:
        '''
        # first convert the connected fields into str. Otherwise the insert into table fails
        self.con.get_cursor()
        if res.get('Employee Details',[]):
            employee_urls = [re.sub(linkedin_url_clean_regex,'',com_dic.get('linkedin_url','')) + '{}' +
                             com_dic.get('Name','') + '{}' + com_dic.get('Designation','')
                             for com_dic in res['Employee Details']]
            res['Employee Details'] = '|'.join(employee_urls)
        else:
            employee_urls = []
            res['Employee Details'] = ''
        if res.get('Also Viewed Companies',[]):
            also_viewed_urls = [re.sub(linkedin_url_clean_regex,'',com_dic.get('company_linkedin_url','')) + '{}' +
                                com_dic.get('Company Name','')
                                for com_dic in res['Also Viewed Companies']]
            res['Also Viewed Companies'] = '|'.join(also_viewed_urls)
        else:
            also_viewed_urls = []
            res['Also Viewed Companies'] = ''
        res_fields = []
        for field in self.table_fields:
            field_val = res.get(field,'NULL')
            res_fields.append(field_val)
        # res_query = "'" + "','".join(res_fields) + "'"  # 'value1','value2' format
        # res_query = ",".join(res_fields)
        # query = u'INSERT INTO linkedin_company_base VALUES ({}) ON CONFLICT DO NOTHING'.format(res_query)
        # self.con.cursor.execute('''INSERT INTO linkedin_company_base
        #                 VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ) ON CONFLICT DO NOTHING''',
        #                  (res_fields[0],res_fields[1],res_fields[2],res_fields[3],res_fields[4],res_fields[5],res_fields[6],
        #                 res_fields[7],res_fields[8],res_fields[9],res_fields[10],res_fields[11]))
        query = '''INSERT INTO {} ({}) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ) '''.format(self.base_table,','.join(self.table_field_names))
        self.con.cursor.execute(query,res_fields)
        # add the url to finished url
        # finished_query = u"INSERT INTO linkedin_company_finished_urls VALUES ('{}') ON CONFLICT DO NOTHING".format(res['Linkedin URL'])
        self.con.cursor.execute("INSERT INTO linkedin_company_finished_urls VALUES (%s) ON CONFLICT DO NOTHING",(res['Linkedin URL'],))
        self.con.cursor.execute("DELETE FROM {} WHERE url = %s".format(self.urls_to_crawl_table),(res['Linkedin URL'],))
        # get list of company urls to crawl and add them
        # urls = []
        # for com_dic in res['Also Viewed Companies']:
        #     urls.append(re.sub(linkedin_url_clean_regex,'',com_dic['company_linkedin_url']))
        if also_viewed_urls:
            # tmp_query = "'" + "','".join(also_viewed_urls) + "'"
            # tmp_query = ",".join(also_viewed_urls)
            # query = 'select url from linkedin_company_finished_urls where url in ({})'.format(tmp_query)
            # self.con.execute(query)
            self.con.cursor.execute(self.con.cursor.mogrify('SELECT url FROM linkedin_company_finished_urls WHERE url IN %s', (tuple(also_viewed_urls),)))
            already_crawled_urls = self.con.cursor.fetchall()
            already_crawled_urls = [i[0] for i in already_crawled_urls]
            urls_to_crawl = list(set(also_viewed_urls)-set(already_crawled_urls))
            if urls_to_crawl:
                # for url in urls_to_crawl:
                #     if url:
                #         # query = u"INSERT INTO linkedin_company_urls_to_crawl VALUES ('{}') ON CONFLICT DO NOTHING".format(url)
                #         self.con.cursor.execute("INSERT INTO linkedin_company_urls_to_crawl VALUES (%s) ON CONFLICT DO NOTHING",(url,))
                # insert all urls together
                records_list_template = ','.join(['%s']*len(urls_to_crawl))
                insert_query = 'INSERT INTO {} VALUES {} ON CONFLICT DO NOTHING'.format(self.urls_to_crawl_table,records_list_template)
                urls_to_crawl1 = [(i,) for i in urls_to_crawl]
                self.con.cursor.execute(insert_query, urls_to_crawl1)
        # get list of people urls to crawl and add them
        # urls = []
        # for com_dic in res['Employee Details']:
        #     urls.append(re.sub(linkedin_url_clean_regex,'',com_dic['linkedin_url']))
        if employee_urls:
            # tmp_query = "'" + "','".join(employee_urls) + "'"
            # query = 'select url from linkedin_people_finished_urls where url in ({})'.format(tmp_query)
            # self.con.execute(query)
            self.con.cursor.execute(self.con.cursor.mogrify('SELECT url FROM linkedin_people_finished_urls WHERE url IN %s', (tuple(employee_urls),)))
            already_crawled_urls = self.con.cursor.fetchall()
            already_crawled_urls = [i[0] for i in already_crawled_urls]
            urls_to_crawl = list(set(employee_urls)-set(already_crawled_urls))
            if urls_to_crawl:
                # for url in urls_to_crawl:
                #     if url:
                #         # query = u"INSERT INTO linkedin_people_urls_to_crawl VALUES ('{}') ON CONFLICT DO NOTHING".format(url)
                #         self.con.cursor.execute("INSERT INTO linkedin_people_urls_to_crawl VALUES (%s) ON CONFLICT DO NOTHING",(url,))
                # inserting all urls together
                records_list_template = ','.join(['%s']*len(urls_to_crawl))
                insert_query = 'INSERT INTO {} VALUES {} ON CONFLICT DO NOTHING'.format(self.urls_to_crawl_table_people,records_list_template)
                urls_to_crawl1 = [(i,) for i in urls_to_crawl]
                self.con.cursor.execute(insert_query, urls_to_crawl1)
        self.con.commit()
        self.con.close_cursor()
        logging.info('saved to database for url:{}'.format(res['Linkedin URL']))

    def worker_save_res(self):
        '''
        :return:
        '''
        while self.run_write_queue:
            res = self.out_queue.get()
            if self.use_db:
                self.save_to_table(res)
            else:
                if res is None:
                    break
                with open(self.out_loc,'a') as f:
                    f.write(str(res)+'\n')
            self.out_queue.task_done()

    def run(self,inp_list,out_loc,log_file_loc,n_threads=2,limit_no=2000,urls_to_crawl_table='linkedin_company_urls_to_crawl',
            urls_to_crawl_priority='linkedin_company_urls_to_crawl_priority',base_table='linkedin_people_base',
            urls_to_crawl_table_people = 'linkedin_people_urls_to_crawl'):
        '''
        :param inp_list:
        :param out_loc:
        :return:
        '''
        logging.basicConfig(filename=log_file_loc, level=logging.INFO,format='%(asctime)s %(message)s')
        if self.use_db:
            self.urls_to_crawl_table = urls_to_crawl_table
            self.urls_to_crawl_priority = urls_to_crawl_priority
            self.base_table = base_table
            self.urls_to_crawl_table_people = urls_to_crawl_table_people
            self.con.get_cursor()
            # first look at priority table
            query = 'select url from {} limit {}'.format(self.urls_to_crawl_priority,limit_no)
            self.con.cursor.execute(query)
            inp_list_priority = self.con.cursor.fetchall()
            limit_no = limit_no - len(inp_list_priority)
            # deleting the urls from priority table and inserting to base urls_to_crawl table
            if inp_list_priority:
                records_list_template = ','.join(['%s']*len(inp_list_priority))
                query = 'DELETE FROM {} WHERE url in ({})'.format(self.urls_to_crawl_priority,records_list_template)
                self.con.cursor.execute(query, inp_list_priority)
                # self.con.commit()
                query = 'INSERT INTO {} VALUES {} ON CONFLICT DO NOTHING'.format(self.urls_to_crawl_table,records_list_template)
                self.con.cursor.execute(query, inp_list_priority)
                self.con.commit()
            if limit_no >0 :
                query = 'select url from {} offset floor(random() * (select count(*) from {})) limit {}'.format(self.urls_to_crawl_table,self.urls_to_crawl_table,limit_no)
                # query = 'select url from linkedin_company_urls_to_crawl limit {}'.format(limit_no)
                self.con.cursor.execute(query)
                inp_list = self.con.cursor.fetchall()
            else:
                inp_list = []
            # creating final list
            inp_list = inp_list_priority+inp_list
            inp_list = [i[0] for i in inp_list]
            self.con.close_cursor()
        self.run_queue = True
        self.run_write_queue = True
        self.out_loc = out_loc
        self.in_queue = Queue(maxsize=0)
        self.out_queue = Queue(maxsize=0)
        # self.processed_queue = Queue(maxsize=0)
        # self.error_queue = Queue(maxsize=0)
        self.gen_proxies()
        for i in range(n_threads):
            worker = threading.Thread(target=self.worker_fetch_url)
            worker.setDaemon(True)
            worker.start()
        worker = threading.Thread(target=self.worker_save_res)
        worker.setDaemon(True)
        worker.start()
        for i in inp_list:
            self.in_queue.put(i)
        del inp_list
        # self.in_queue.join()
        #doing while loop instead of join to save all the results in the queue. not sure if this is working or not.
        while not self.in_queue.empty():
            try:
                time.sleep(2)
            except :
                # self.run_queue = False
                break
        # time.sleep(20) #giving 20 second wait for all existing tasks to finish
        # if using db, put remaining urls back to table
        # if self.use_db:
        #     remaining_urls = list(self.in_queue.queue)
        #     if remaining_urls:
        #         logging.info('Crawling stopped. Put remaining urls back to table. No of urls remaining:'.format(len(remaining_urls)))
        #         self.con.get_cursor()
        #         records_list_template = ','.join(['%s']*len(remaining_urls))
        #         query = 'INSERT INTO linkedin_company_urls_to_crawl VALUES {0} ON CONFLICT DO NOTHING'.format(records_list_template)
        #         remaining_urls1 = [(i,) for i in remaining_urls]
        #         self.con.cursor.execute(query, remaining_urls1)
        #         self.con.close_cursor()
        logging.info('crawling stopped, trying to save already crawled results. No of results left in out queue : {}'.format(len(self.out_queue.queue)))
        self.out_queue.join()
        self.run_queue = False
        self.run_write_queue = False
        self.proxy_generator.exit()
        logging.info('Finished. No of results left in out queue : {}'.format(len(self.out_queue.queue)))
        # logging._removeHandlerRef()
        time.sleep(5)


class LinkedinProfileCrawlerThread(object):
    def __init__(self,browser='Firefox',visible=True,proxy=False,use_tor=False,use_db=False):
        '''
        :param browser:
        :param visible:
        :param proxy:
        :param use_tor: if use_tor is True, proxy should be False
        :return:
        '''
        self.browser = browser
        self.visible = visible
        self.proxy = proxy
        self.ip_matcher = re.compile("^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")
        self.use_tor = use_tor
        self.use_db = use_db
        if use_db:
            self.con = PostgresConnect()
            self.table_fields = ['Linkedin URL','Name','Position','Location','Company','CompanyLinkedinPage',
                                 'PreviousCompanies','Education','Industry','Summary','Skills','Experience'
                                 ,'Related People','Same Name People']
            self.table_field_names = ['linkedin_url','name','sub_text','location','company_name','company_linkedin_url',
                                      'previous_companies','education','industry','summary','skills','experience',
                                      'related_people','same_name_people']
        if proxy:
            self.proxy_generator = ProxyGen(browser=browser,visible=visible,page_load_timeout=25)
            self.proxies = Queue(maxsize=0)
            # self.proxies.put((None,None)) #try with actual ip first time
            # self.gen_proxies() #moving this to run call due to logging problem

    def gen_proxies(self):
        '''
        :return:
        '''
        if not self.proxy:
            return [(None,None)]
        else:
            try:
                proxies = self.proxy_generator.generate_proxy()
            except Exception :
                logging.exception('could not create proxies. using None')
                proxies = [(None,None)]
        logging.info('Proxies fetched {}'.format(proxies))
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
        return self.proxies.get()

    def worker_fetch_url(self):
        '''
        :return:
        '''
        try: #some error happens while getting proxy sometimes. putting it in try
            proxy_dets = self.get_proxy()
            logging.info('proxy to be used : {}'.format(proxy_dets))
            proxy_ip,proxy_port = proxy_dets[0],proxy_dets[1]
            crawler = linkedin_profile_crawler.LinkedinProfileCrawler(self.browser,self.visible,proxy=self.proxy,
                                                                      proxy_ip=proxy_ip,proxy_port=proxy_port,use_tor=self.use_tor)
        except: #if  coming here, run without proxy
            logging.info('Error while getting proxy. Running without proxy')
            proxy_ip,proxy_port = None,None
            crawler = linkedin_profile_crawler.LinkedinProfileCrawler(self.browser,self.visible,proxy=False,
                                                                      proxy_ip=proxy_ip,proxy_port=proxy_port,use_tor=self.use_tor)
        def get_output(crawler,url,res_1,event):
            res_1['result'] = crawler.fetch_details_urlinput(url)
            event.set()
        no_errors = 0
        ind = 0
        n_blocks =0
        while self.run_queue:
            ind += 1
            url = self.in_queue.get()
            logging.info('Input URL:{}, thread:{}'.format(url,threading.currentThread()))
            try:
                time.sleep(randint(1,4))
                res_1 = {}
                event = threading.Event()
                t1 = threading.Thread(target=get_output, args=(crawler,url,res_1,event,))
                t1.daemon = True
                t1.start()
                event.wait(timeout=30)
                if res_1 is None: #if None means timeout happened, push to queue again
                    self.in_queue.put(url)
                    # no_errors += 1
                elif 'result' in res_1:
                    res = res_1['result']
                    if res:
                        if 'Name' in res :
                            if res['Name'] and res['Name'] != 'LinkedIn':
                                self.out_queue.put(res)
                                no_errors = 0
                                n_blocks = 0
                                # if res['Name'] not in self.processed_queue.queue:
                                #     self.processed_queue.put(res['Name'])
                                #     self.out_queue.put(res)
                                #     no_errors = 0
                                #     n_blocks = 0
                                # else:
                                #     logging.info('Duplicate name while processing url:'+url+'. Duplicate value:'+res['Name'])
                                #     no_errors += 1
                            else:
                                no_errors += 1
                        elif 'Notes' in res:
                            if res['Notes'] == 'Not Available Pubicly':
                                # self.out_queue.put(res)
                                pass
                            elif res['Notes'] == 'Java script code':
                                # self.out_queue.put(res)
                                pass
                        else:
                            no_errors += 1
                    else:
                        no_errors += 1
                else:
                    no_errors += 1
            except Exception :
                logging.exception('Error while execution for url: {0}, Thread: {1}'.format(url,threading.currentThread()))
                time.sleep(1)
                no_errors += 1
            if ind%10 == 0:
                time.sleep(randint(2,6))
                if ind%100 == 0:
                    time.sleep(randint(10,20))
            if no_errors == 6:
                no_errors = no_errors - 1
                if self.use_tor:
                    pass
                elif not self.proxy:
                    n_blocks += 1
                    logging.info('Error condition met, sleeping for '+str(min(n_blocks,6)*600)+' seconds')
                    time.sleep(min(n_blocks,6)*600)
                else:
                    logging.info('Error condition met, trying to use another ip. Thread: {0}, Current ip: {1}'.format(threading.currentThread(), proxy_dets))
                    try:
                        crawler.exit()
                        proxy_dets = self.get_proxy()
                        logging.info('proxy to be used: {0}, Thread:{1}'.format(proxy_dets,threading.currentThread()))
                        proxy_ip,proxy_port = proxy_dets[0],proxy_dets[1]
                        crawler.init_selenium_parser(self.browser,self.visible,proxy=self.proxy,
                                                            proxy_ip=proxy_ip,proxy_port=proxy_port,use_tor=self.use_tor)
                    except:
                        logging.exception('Exception while trying to change ip, use same parser, thread:{}'.format(threading.currentThread()))
                        try:
                            crawler.init_selenium_parser() #try with already existing parameters
                        except:
                            logging.exception('Exception, can not restart crawler with already existing parameters, trying to restart, thread:{}'.format(threading.currentThread()))
                            try:
                                crawler = linkedin_profile_crawler.LinkedinProfileCrawler(self.browser,self.visible,proxy=self.proxy,
                                                                      proxy_ip=proxy_ip,proxy_port=proxy_port,use_tor=self.use_tor)
                            except:
                                logging.exception('could not start crawler')
            if no_errors>0:
                logging.info('Something went wrong, could not fetch details for url: {}, thread id: {}'.format(url,threading.currentThread()))
            self.in_queue.task_done()
        logging.info('exiting crawler, thread:{}'.format(threading.currentThread()))
        crawler.exit()
        logging.info('crawler exited, thread:{}'.format(threading.currentThread()))

    def save_to_table(self,res):
        '''
        :param res:
        :return:
        '''
        # company linkedin page needst to be added to urls to crawl
        # first convert the connected fields into str. Otherwise the insert into table fails
        self.con.get_cursor()
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
        res_fields = []
        for field in self.table_fields:
            field_val = res.get(field,'NULL')
            res_fields.append(field_val)
        # res_query = "'" + "','".join(res_fields) + "'"  # 'value1','value2' format
        # res_query = ",".join(res_fields)
        # query = u'INSERT INTO linkedin_company_base VALUES ({}) ON CONFLICT DO NOTHING'.format(res_query)
        # self.con.cursor.execute('''INSERT INTO linkedin_people_base
        #                 VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ) ON CONFLICT DO NOTHING''',
        #                  (res_fields[0],res_fields[1],res_fields[2],res_fields[3],res_fields[4],res_fields[5],res_fields[6],
        #                 res_fields[7],res_fields[8],res_fields[9],res_fields[10],res_fields[11]))
        query = 'INSERT INTO {} ({}) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ) '.format(self.base_table,','.join(self.table_field_names))
        self.con.cursor.execute(query,res_fields)
        # add the url to finished url
        # finished_query = u"INSERT INTO linkedin_company_finished_urls VALUES ('{}') ON CONFLICT DO NOTHING".format(res['Linkedin URL'])
        self.con.cursor.execute("INSERT INTO linkedin_people_finished_urls VALUES (%s) ON CONFLICT DO NOTHING",(res['Linkedin URL'],))
        self.con.cursor.execute("DELETE FROM {} WHERE url = %s".format(self.urls_to_crawl_table),(res['Linkedin URL'],))
        # get list of company urls to crawl and add them
        # urls = []
        # for com_dic in res['Also Viewed Companies']:
        #     urls.append(re.sub(linkedin_url_clean_regex,'',com_dic['company_linkedin_url']))
        if related_people_urls or same_name_urls:
            # tmp_query = "'" + "','".join(also_viewed_urls) + "'"
            # tmp_query = ",".join(also_viewed_urls)
            # query = 'select url from linkedin_company_finished_urls where url in ({})'.format(tmp_query)
            # self.con.execute(query)
            people_urls = list(set(related_people_urls+same_name_urls))
            self.con.cursor.execute(self.con.cursor.mogrify('SELECT url FROM linkedin_people_finished_urls WHERE url IN %s', (tuple(people_urls),)))
            already_crawled_urls = self.con.cursor.fetchall()
            already_crawled_urls = [i[0] for i in already_crawled_urls]
            urls_to_crawl = list(set(people_urls)-set(already_crawled_urls))
            if urls_to_crawl:
                # for url in urls_to_crawl:
                #     if url:
                #         # query = u"INSERT INTO linkedin_company_urls_to_crawl VALUES ('{}') ON CONFLICT DO NOTHING".format(url)
                #         self.con.cursor.execute("INSERT INTO linkedin_people_urls_to_crawl VALUES (%s) ON CONFLICT DO NOTHING",(url,))
                records_list_template = ','.join(['%s']*len(urls_to_crawl))
                insert_query = 'INSERT INTO {} VALUES {} ON CONFLICT DO NOTHING'.format(self.urls_to_crawl_table,records_list_template)
                urls_to_crawl1 = [(i,) for i in urls_to_crawl]
                self.con.cursor.execute(insert_query, urls_to_crawl1)
        # get list of people urls to crawl and add them
        # urls = []
        # for com_dic in res['Employee Details']:
        #     urls.append(re.sub(linkedin_url_clean_regex,'',com_dic['linkedin_url']))
        if company_urls:
            # tmp_query = "'" + "','".join(employee_urls) + "'"
            # query = 'select url from linkedin_people_finished_urls where url in ({})'.format(tmp_query)
            # self.con.execute(query)
            self.con.cursor.execute(self.con.cursor.mogrify('SELECT url FROM linkedin_company_finished_urls WHERE url IN %s', (tuple(company_urls),)))
            already_crawled_urls = self.con.cursor.fetchall()
            already_crawled_urls = [i[0] for i in already_crawled_urls]
            urls_to_crawl = list(set(company_urls)-set(already_crawled_urls))
            if urls_to_crawl:
                # for url in urls_to_crawl:
                #     if url:
                #         # query = u"INSERT INTO linkedin_people_urls_to_crawl VALUES ('{}') ON CONFLICT DO NOTHING".format(url)
                #         self.con.cursor.execute("INSERT INTO linkedin_company_urls_to_crawl VALUES (%s) ON CONFLICT DO NOTHING",(url,))
                records_list_template = ','.join(['%s']*len(urls_to_crawl))
                insert_query = 'INSERT INTO {} VALUES {} ON CONFLICT DO NOTHING'.format(self.urls_to_crawl_table_company,records_list_template)
                urls_to_crawl1 = [(i,) for i in urls_to_crawl]
                self.con.cursor.execute(insert_query, urls_to_crawl1)
        self.con.commit()
        self.con.close_cursor()
        logging.info('saved to database for url:{}'.format(res['Linkedin URL']))


    def worker_save_res(self):
        '''
        :return:
        '''
        while self.run_write_queue:
            res = self.out_queue.get()
            if self.use_db:
                self.save_to_table(res)
            else:
                if res is None:
                    break
                with open(self.out_loc,'a') as f:
                    f.write(str(res)+'\n')
            self.out_queue.task_done()

    def run(self,inp_list,out_loc,log_file_loc,n_threads=2,limit_no=2000,urls_to_crawl_table='linkedin_people_urls_to_crawl',
            urls_to_crawl_priority='linkedin_people_urls_to_crawl_priority',base_table='linkedin_people_base',
            urls_to_crawl_table_company = 'linkedin_company_urls_to_crawl'):
        '''
        :param inp_list:
        :param out_loc:
        :return:
        '''
        logging.basicConfig(filename=log_file_loc, level=logging.INFO,format='%(asctime)s %(message)s')
        if self.use_db:
            self.urls_to_crawl_table = urls_to_crawl_table
            self.urls_to_crawl_priority = urls_to_crawl_priority
            self.base_table = base_table
            self.urls_to_crawl_table_company = urls_to_crawl_table_company
            self.con.get_cursor()
            # first look at priority table
            query = 'select url from {} limit {}'.format(self.urls_to_crawl_priority,limit_no)
            self.con.cursor.execute(query)
            inp_list_priority = self.con.cursor.fetchall()
            limit_no = limit_no - len(inp_list_priority)
            # deleting the urls from priority table and inserting to base urls_to_crawl table
            if inp_list_priority:
                records_list_template = ','.join(['%s']*len(inp_list_priority))
                query = 'DELETE FROM {} WHERE url in ({})'.format(self.urls_to_crawl_priority,records_list_template)
                self.con.cursor.execute(query, inp_list_priority)
                # self.con.commit()
                query = 'INSERT INTO {} VALUES {} ON CONFLICT DO NOTHING'.format(self.urls_to_crawl_table,records_list_template)
                self.con.cursor.execute(query, inp_list_priority)
                self.con.commit()
            if limit_no>0:
                # query = 'SELECT url FROM linkedin_people_urls_to_crawl LIMIT {}'.format(10)
                query = 'select url from {} offset floor(random() * (select count(*) from {})) limit {}'.format(self.urls_to_crawl_table,self.urls_to_crawl_table,limit_no)
                self.con.cursor.execute(query)
                inp_list = self.con.cursor.fetchall()
            else:
                inp_list = []
            # creating final list
            inp_list = inp_list_priority+inp_list
            inp_list = [i[0] for i in inp_list]
            # records_list_template = ','.join(['%s']*len(inp_list))
            # query = 'DELETE FROM linkedin_people_urls_to_crawl WHERE url in ({})'.format(records_list_template)
            # self.con.cursor.execute(query, inp_list)
            # self.con.commit()
            self.con.close_cursor()
        self.run_queue = True
        self.run_write_queue = True
        self.out_loc = out_loc
        self.in_queue = Queue(maxsize=0)
        self.out_queue = Queue(maxsize=0)
        # self.processed_queue = Queue(maxsize=0)
        self.gen_proxies()
        for i in range(n_threads):
            worker = threading.Thread(target=self.worker_fetch_url)
            worker.setDaemon(True)
            worker.start()
        worker = threading.Thread(target=self.worker_save_res)
        worker.setDaemon(True)
        worker.start()
        for i in inp_list:
            self.in_queue.put(i)
        # self.in_queue.join()
        #doing while loop instead of join to save all the results in the queue. not sure if this is working or not.
        while not self.in_queue.empty():
            try:
                time.sleep(2)
                # if self.use_db:
                #     if len(self.in_queue.queue)<10:
                #         self.con.get_cursor()
                #         query = 'SELECT url FROM linkedin_people_urls_to_crawl LIMIT {}'.format(10)
                #         self.con.cursor.execute(query)
                #         inp_list = self.con.cursor.fetchall()
                #         inp_list = [i[0] for i in inp_list]
                #         records_list_template = ','.join(['%s']*len(inp_list))
                #         query = 'DELETE FROM linkedin_people_urls_to_crawl WHERE url in ({})'.format(records_list_template)
                #         self.con.cursor.execute(query, inp_list)
                #         self.con.commit()
                #         self.con.close_cursor()
                #         for i in inp_list:
                #             self.in_queue.put(i)
                # else:
                #     time.sleep(2)
            except :
                # self.run_queue = False
                break
        # time.sleep(20) #giving 20 second wait for all existing tasks to finish
        logging.info('crawling stopped, trying to save already crawled results. No of results left in out queue : {}'.format(len(self.out_queue.queue)))
        self.out_queue.join()
        # if using db, we need to put the remaining urls in the in_queue back to table
        # if self.use_db:
        #     remaining_urls = list(self.in_queue.queue)
        #     # logging.info('remaining urls:{}'.format(remaining_urls))
        #     if remaining_urls:
        #         logging.info('Crawling stopped.Put remaining urls back to table. No of urls remaining:{}'.format(len(remaining_urls)))
        #         try:
        #             self.con.get_cursor()
        #         except:
        #             logging.exception('error while getting cursor')
        #         # logging.info('cursor got')
        #         records_list_template = ','.join(['%s']*len(remaining_urls))
        #         query = 'INSERT INTO linkedin_people_urls_to_crawl VALUES {0} ON CONFLICT DO NOTHING'.format(records_list_template)
        #         remaining_urls1 = [(i,) for i in remaining_urls]
        #         # logging.info('query:{},remaining_urls1:{}'.format(query,remaining_urls1))
        #         try:
        #             self.con.cursor.execute(query, remaining_urls1)
        #         except:
        #             logging.exception('error while inserting')
        #         self.con.commit()
        #         self.con.close_cursor()
        #         logging.info('completed putting remaining urls')
        self.run_queue = False
        self.run_write_queue = False
        self.proxy_generator.exit()
        logging.info('Finished. No of results left in out queue : {}'.format(len(self.out_queue.queue)))
        # logging._removeHandlerRef()
        time.sleep(5)

