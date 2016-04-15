__author__ = 'joswin'

from Queue import Queue
import threading
import logging
import time
from random import randint

import linkedin_company_crawler,linkedin_profile_crawler

class LinkedinCompanyCrawlerThread(object):
    def __init__(self,browser='Firefox',visible=True):
        '''
        '''
        self.browser = browser
        self.visible = visible

    def worker_fetch_url(self):
        '''
        :return:
        '''
        company_crawler = linkedin_company_crawler.LinkedinOrganizationService(self.browser,self.visible)
        def get_output(url,res_1,event):
            logging.info('get_output function before fetching- url:{},res_1:{}'.format(url,res_1))
            res_1['result'] = company_crawler.get_organization_details_from_linkedin_link(url)
            logging.info('get_output function after fetching- url:{},res_1:{}'.format(url,res_1))
            event.set()
        no_errors = 0
        ind = 0
        n_blocks = 0
        while True:
            ind += 1
            url = self.in_queue.get()
            logging.info('Input URL:'+url)
            try:
                time.sleep(randint(1,4))
                res_1 = {}
                event = threading.Event()
                t1 = threading.Thread(target=get_output, args=(url,res_1,event,))
                t1.daemon = True
                t1.start()
                event.wait(timeout=30)
                logging.info('Fetched details after event wait - res_1:{}'.format(res_1))
                if res_1 is None: #if None means timeout happened, push to queue again
                    self.in_queue.put(url)
                    # no_errors += 1
                elif 'result' in res_1:
                    res = res_1['result']
                    if res:
                        if 'Company Name' in res :
                            if res['Company Name'] and res['Company Name'] != 'LinkedIn':
                                if res['Company Name'] not in self.processed_queue.queue:
                                    self.processed_queue.put(res['Company Name'])
                                    self.out_queue.put(res)
                                    no_errors = 0
                                    n_blocks = 0
                                else:
                                    logging.info('Duplicate name while processing url:'+url+'. Duplicate value:'+res['Company Name'])
                                    no_errors += 1
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
            except Exception as e:
                logging.exception('Error while execution for url: '+url+', sleeping 2 seconds')
                time.sleep(2)
                no_errors += 1
            if ind%10 == 0:
                time.sleep(randint(8,12))
                if ind%100 == 0:
                    time.sleep(randint(25,35))
            if no_errors >= 6:
                n_blocks += 1
                logging.info('Error condition met, sleeping for '+str(n_blocks*600)+' seconds')
                time.sleep(n_blocks*600)
                no_errors = no_errors - 1
            self.in_queue.task_done()

    def worker_save_res(self):
        '''
        :return:
        '''
        while True:
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
        self.out_loc = out_loc
        self.out_queue = Queue(maxsize=0)
        self.processed_queue = Queue(maxsize=0)
        for i in range(n_threads):
            worker = threading.Thread(target=self.worker_fetch_url)
            worker.setDaemon(True)
            worker.start()
        worker = threading.Thread(target=self.worker_save_res)
        worker.setDaemon(True)
        worker.start()
        self.in_queue = Queue(maxsize=0)
        for i in inp_list:
            self.in_queue.put(i)
        del inp_list
        self.in_queue.join()
        self.out_queue.join()
        logging.info('Finished')

class LinkedinProfileCrawlerThread(object):
    def __init__(self,browser='Firefox',visible=True):
        '''
        '''
        self.browser = browser
        self.visible = visible

    def worker_fetch_url(self):
        '''
        :return:
        '''
        crawler = linkedin_profile_crawler.LinkedinProfileCrawler(self.browser,self.visible)
        def get_output(url,res_1,event):
            res_1['result'] = crawler.fetch_details_urlinput(url)
            event.set()
        no_errors = 0
        ind = 0
        n_blocks =0
        while True:
            ind += 1
            url = self.in_queue.get()
            logging.info('Input URL:'+url)
            try:
                time.sleep(randint(1,4))
                res_1 = {}
                event = threading.Event()
                t1 = threading.Thread(target=get_output, args=(url,res_1,event,))
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
                                if res['Name'] not in self.processed_queue.queue:
                                    self.processed_queue.put(res['Name'])
                                    self.out_queue.put(res)
                                    no_errors = 0
                                    n_blocks = 0
                                else:
                                    logging.info('Duplicate name while processing url:'+url+'. Duplicate value:'+res['Name'])
                                    no_errors += 1
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
            except Exception as e:
                logging.exception('Error while execution for url: '+url+', sleeping 2 seconds')
                time.sleep(2)
                no_errors += 1
            if ind%10 == 0:
                time.sleep(randint(8,12))
                if ind%100 == 0:
                    time.sleep(randint(25,35))
            if no_errors == 6:
                n_blocks += 1
                logging.info('Error condition met, sleeping for '+str(n_blocks*600)+' seconds')
                time.sleep(n_blocks*600)
                no_errors = no_errors - 1
            self.in_queue.task_done()

    def worker_save_res(self):
        '''
        :return:
        '''
        while True:
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
        self.out_loc = out_loc
        self.out_queue = Queue(maxsize=0)
        self.processed_queue = Queue(maxsize=0)
        for i in range(n_threads):
            worker = threading.Thread(target=self.worker_fetch_url)
            worker.setDaemon(True)
            worker.start()
        worker = threading.Thread(target=self.worker_save_res)
        worker.setDaemon(True)
        worker.start()
        self.in_queue = Queue(maxsize=0)
        for i in inp_list:
            self.in_queue.put(i)
        del inp_list
        self.in_queue.join()
        self.out_queue.join()
        logging.info('Finished')

