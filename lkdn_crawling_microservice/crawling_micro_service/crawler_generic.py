__author__ = 'joswin'

import datetime
import time
import re
import pickle
import multiprocessing
import sys
import gc
import logging

import crawler

class LinkedinCrawlerThread(object):
    def __init__(self):
        '''
        '''
        pass

    def run_organization_crawler_single(self,res_file=None,log_file=None,crawled_loc='crawled_res/',browser='Firefox',
                                        visible=False,proxy=True,url_file='company_urls_to_crawl_18April.pkl',
                                        n_threads=6,use_tor=False,use_db=False,limit_no=2000,urls_to_crawl_table='linkedin_company_urls_to_crawl',
            urls_to_crawl_priority='linkedin_company_urls_to_crawl_priority',base_table='linkedin_people_base',
            urls_to_crawl_table_people = 'linkedin_people_urls_to_crawl',finished_urls_table_company = 'linkedin_company_finished_urls',
            finished_urls_table_people = 'linkedin_people_finished_urls',list_id=None):
        '''
        :param res_file:
        :param log_file:
        :param crawled_loc:
        :param browser:
        :param visible:
        :param proxy:
        :param url_file:
        :param n_threads:
        :param use_tor:
        :param use_db:
        :return:
        '''
        if res_file is None:
            res_file = crawled_loc+'company_crawled_'+re.sub(' ','_',str(datetime.datetime.now())[:-13])+'.txt'
        if log_file is None:
            log_file = crawled_loc+'logs/company_crawling'+re.sub(' ','_',str(datetime.datetime.now())[:-13])+'.log'
        if use_db:
            urls = []
        else:
            with open(url_file,'r') as f:
                urls = pickle.load(f)
        cc = crawler.LinkedinCompanyCrawlerThread(browser,visible=visible,proxy=proxy,use_tor=use_tor,use_db=use_db)
        gc.collect()
        cc.run(urls,res_file,log_file,n_threads,limit_no=limit_no,urls_to_crawl_table=urls_to_crawl_table,
            urls_to_crawl_priority=urls_to_crawl_priority,base_table=base_table,
            urls_to_crawl_table_people = urls_to_crawl_table_people,finished_urls_table_company = finished_urls_table_company,
            finished_urls_table_people = finished_urls_table_people,list_id=list_id)

    def run_people_crawler_single(self,res_file=None,log_file=None,crawled_loc='crawled_res/',browser='Firefox',
                                        visible=False,proxy=True,url_file='people_urls_to_crawl_18April.pkl',
                                        n_threads=6,use_tor=False,use_db=False,limit_no=2000,urls_to_crawl_table='linkedin_people_urls_to_crawl',
            urls_to_crawl_priority='linkedin_people_urls_to_crawl_priority',base_table='linkedin_people_base',
            urls_to_crawl_table_company = 'linkedin_company_urls_to_crawl',finished_urls_table_company = 'linkedin_company_finished_urls',
            finished_urls_table_people = 'linkedin_people_finished_urls',list_id=''):
        '''
        :param res_file:
        :param log_file:
        :param crawled_loc:
        :param browser:
        :param visible:
        :param proxy:
        :param url_file:
        :param n_threads:
        :param use_tor:
        :param use_db:
        :return:
        '''
        if res_file is None:
            res_file = crawled_loc+'people_crawled_'+re.sub(' ','_',str(datetime.datetime.now())[:-13])+'.txt'
        if log_file is None:
            log_file = crawled_loc+'logs/people_crawling'+re.sub(' ','_',str(datetime.datetime.now())[:-13])+'.log'
        if use_db:
            urls = []
        else:
            with open(url_file,'r') as f:
                urls = pickle.load(f)
        cc = crawler.LinkedinProfileCrawlerThread(browser,visible=visible,proxy=proxy,use_tor=use_tor,use_db=use_db)
        gc.collect()
        cc.run(urls,res_file,log_file,n_threads,limit_no=limit_no,urls_to_crawl_table=urls_to_crawl_table,
            urls_to_crawl_priority=urls_to_crawl_priority,base_table=base_table,
            urls_to_crawl_table_company = urls_to_crawl_table_company,finished_urls_table_company = finished_urls_table_company,
            finished_urls_table_people = finished_urls_table_people,list_id=list_id)

    def gen_url_lists(self,crawled_loc = 'crawled_res/',limit_no=30000,ignore_urls=[],add_urls_people=[],add_urls_company=[]):
        '''this function not used now
        '''
        pass

    def run_both_single(self,crawled_loc='crawled_res/',browser = 'Firefox',visible=False,
                         proxy=True,limit_no=100,n_threads=2,use_tor=False,use_db=True,
                         urls_to_crawl_people='crawler.linkedin_people_urls_to_crawl',
                         urls_to_crawl_people_priority='crawler.linkedin_people_urls_to_crawl_priority',
                         people_base_table='crawler.linkedin_people_base',
                         urls_to_crawl_company = 'crawler.linkedin_company_urls_to_crawl',
                         urls_to_crawl_company_priority='crawler.linkedin_company_urls_to_crawl_priority',
                         company_base_table='crawler.linkedin_company_base',
                         finished_urls_table_company = 'crawler.linkedin_company_finished_urls',
                         finished_urls_table_people = 'crawler.linkedin_people_finished_urls',
                         list_id = None):
        '''
        :param crawled_loc: location of result (used when crawling done using files). If crawling is db based, the logs
                            will go to logs folder in crawled_loc. This folder needs to be created before starting run
        :param browser: Browser to be used in selenium. Support for Firefox and phantomjs currently
        :param visible: Run in background if False
        :param proxy: Use proxies if True. This does not work properly. But set this to True
        :param limit_no: no of urls in a single run
        :param n_threads: no of threads
        :param use_tor: if True, use tor
        :param use_db: if True, use database (postgres)
        :param urls_to_crawl_people:
        :param urls_to_crawl_people_priority:
        :param people_base_table:
        :param urls_to_crawl_company:
        :param urls_to_crawl_company_priority:
        :param company_base_table:
        :param finished_urls_table_company:
        :param finished_urls_table_people:
        :param list_id:
        :return:
        '''
        logging.info('started crawling process')
        if not list_id:
            raise ValueError('Need to give list id')
        if use_tor:
            proxy = False
        if use_db:
            pass
        else:
            self.gen_url_lists(crawled_loc=crawled_loc,limit_no=limit_no
                               # ,ignore_urls=['company_urls_for_server.pkl','people_urls_for_server.pkl']
            )
            gc.collect()
        # urls_to_crawl_table='linkedin_people_urls_to_crawl',
        #     urls_to_crawl_priority='linkedin_people_urls_to_crawl_priority',base_table='linkedin_people_base',
        #     urls_to_crawl_table_company = 'linkedin_company_urls_to_crawl'
        worker_people = multiprocessing.Process(target=self.run_people_crawler_single,
                                                args=(None,None,crawled_loc,browser,visible,proxy,
                                                'people_urls_to_crawl.pkl',n_threads,use_tor,use_db,limit_no,
                                                urls_to_crawl_people,urls_to_crawl_people_priority,people_base_table,urls_to_crawl_company,
                                                finished_urls_table_company ,finished_urls_table_people,list_id ))
        worker_company = multiprocessing.Process(target=self.run_organization_crawler_single,
                                                 args=(None,None,crawled_loc,browser,visible,proxy,
                                                'company_urls_to_crawl.pkl',n_threads,use_tor,use_db,limit_no,
                                                 urls_to_crawl_company,urls_to_crawl_company_priority,company_base_table,urls_to_crawl_people,
                                                 finished_urls_table_company ,finished_urls_table_people,list_id ))
        gc.collect()
        worker_people.daemon = True
        worker_company.daemon = True
        worker_people.start()
        worker_company.start()
        gc.collect()
        worker_people.join()
        worker_company.join(timeout=600)
        logging.info('finished crawling process')
        if worker_people.is_alive():
            worker_people.terminate()
        if worker_company.is_alive():
            worker_company.terminate()
        time.sleep(10)
        logging.info('exiting from the crawling process')

if __name__ == '__main__':
    cc = LinkedinCrawlerThread()
    if len(sys.argv)<3:
        cc.run_both_single(use_db=True)
    elif len(sys.argv)==3:
        limit_no = sys.argv[1]
        limit_no = int(limit_no)
        n_threads = sys.argv[2]
        n_threads = int(n_threads)
        cc.run_both_single(limit_no=limit_no,n_threads=n_threads,use_db=True)
    elif len(sys.argv) == 4:#fourth argument is for using tor
        limit_no = sys.argv[1]
        limit_no = int(limit_no)
        n_threads = sys.argv[2]
        n_threads = int(n_threads)
        use_tor = sys.argv[3]
        use_tor = (use_tor == 'True')
        cc.run_both_single(limit_no=limit_no,n_threads=n_threads,use_tor=use_tor,use_db=True)
    else: # from fifth argument is the table names
        limit_no = sys.argv[1]
        limit_no = int(limit_no)
        n_threads = sys.argv[2]
        n_threads = int(n_threads)
        use_tor = sys.argv[3]
        use_tor = (use_tor == 'True')
        urls_to_crawl_people = sys.argv[4]
        urls_to_crawl_people_priority = sys.argv[5]
        people_base_table = sys.argv[6]
        urls_to_crawl_company = sys.argv[7]
        urls_to_crawl_company_priority = sys.argv[8]
        company_base_table = sys.argv[9]
        finished_urls_table_company = sys.argv[10]
        finished_urls_table_people = sys.argv[11]
        list_id = sys.argv[12]
        cc.run_both_single(limit_no=limit_no,n_threads=n_threads,use_tor=use_tor,use_db=True,
            urls_to_crawl_people=urls_to_crawl_people,urls_to_crawl_people_priority=urls_to_crawl_people_priority,
            people_base_table=people_base_table,urls_to_crawl_company = urls_to_crawl_company,
            urls_to_crawl_company_priority=urls_to_crawl_company_priority,company_base_table=company_base_table,
            finished_urls_table_company=finished_urls_table_company,finished_urls_table_people=finished_urls_table_people,
            list_id=list_id)


