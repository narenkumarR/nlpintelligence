__author__ = 'joswin'

import re
from Queue import Queue
import threading
import logging
import time
from random import randint

import linkedin_company_crawler,linkedin_profile_crawler
from proxy_generator import ProxyGen

class LinkedinCompanyCrawlerThread(object):
    def __init__(self,browser='Firefox',visible=True,proxy=False,use_tor=False):
        '''
        '''
        self.browser = browser
        self.visible = visible
        self.proxy = proxy
        self.ip_matcher = re.compile("^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")
        self.use_tor = use_tor
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

    def worker_save_res(self):
        '''
        :return:
        '''
        while self.run_write_queue:
            res = self.out_queue.get()
            if res is None:
                break
            with open(self.out_loc,'a') as f:
                f.write(str(res)+'\n')
            self.out_queue.task_done()

    def run(self,inp_list,out_loc,log_file_loc,n_threads=2):
        '''
        :param inp_list:
        :param out_loc:
        :return:
        '''
        logging.basicConfig(filename=log_file_loc, level=logging.INFO,format='%(asctime)s %(message)s')
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
        logging.info('crawling stopped, trying to save already crawled results. No of results left in out queue : {}'.format(len(self.out_queue.queue)))
        self.out_queue.join()
        self.run_queue = False
        self.run_write_queue = False
        self.proxy_generator.exit()
        logging.info('Finished. No of results left in out queue : {}'.format(len(self.out_queue.queue)))
        # logging._removeHandlerRef()
        time.sleep(5)


class LinkedinProfileCrawlerThread(object):
    def __init__(self,browser='Firefox',visible=True,proxy=False,use_tor=False):
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
                        logging.exception('Exception while trying to restart crawler with already existing parameters, thread:{}'.format(threading.currentThread()))
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

    def worker_save_res(self):
        '''
        :return:
        '''
        while self.run_write_queue:
            res = self.out_queue.get()
            with open(self.out_loc,'a') as f:
                f.write(str(res)+'\n')
            self.out_queue.task_done()

    def run(self,inp_list,out_loc,log_file_loc,n_threads=2):
        '''
        :param inp_list:
        :param out_loc:
        :return:
        '''
        logging.basicConfig(filename=log_file_loc, level=logging.INFO,format='%(asctime)s %(message)s')
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
            except :
                # self.run_queue = False
                break
        # time.sleep(20) #giving 20 second wait for all existing tasks to finish
        logging.info('crawling stopped, trying to save already crawled results. No of results left in out queue : {}'.format(len(self.out_queue.queue)))
        self.out_queue.join()
        self.run_queue = False
        self.run_write_queue = False
        self.proxy_generator.exit()
        logging.info('Finished. No of results left in out queue : {}'.format(len(self.out_queue.queue)))
        # logging._removeHandlerRef()
        time.sleep(5)

