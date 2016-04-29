__author__ = 'joswin'

from bs_crawl import BeautifulsoupCrawl
from duckduckgo_crawler import DuckduckgoCrawler
from urlparse import urljoin

import re
import threading
import logging
import time
from Queue import Queue

class CompanyLinkedinURLExtractorSingle(object):
    '''Find url for a single company
    '''
    def __init__(self):
        self.crawler = BeautifulsoupCrawl()
        self.ddg_crawler = DuckduckgoCrawler()

    def get_linkedin_url(self,company_url,time_out=30):
        '''
        :param company_url:
        :return:
        '''
        self.time_out = time_out
        soup = self.crawler.single_wp(company_url)
        urls = self.get_urls_soupinput(soup,company_url)
        linkedin_url,confidence = self.get_linkedin_company_url_listinput(urls,company_url)
        return (linkedin_url,confidence)

    def get_urls_soupinput(self,soup,base_url):
        '''
        :param soup:
        :param base_url:if extracted url is of the form /tag, add the base_url also
        :return:
        '''
        # if base_url[-1] == '/':
        #     base_url = base_url[:-1]
        urls = []
        for i in soup.findAll('a'):
            try:
                tmp_url = i['href']
                if re.search('^mail',tmp_url):
                    continue
                if re.search(r'^/',tmp_url) or not re.search(r'^http|^www',tmp_url):
                    tmp_url = urljoin(tmp_url,base_url)
                urls.append(tmp_url)
            except:
                logging.exception('Error while trying to fetch link for url:{0},link:{1}'.format(base_url,i))
        # urls = [i['href'] for i in soup.findAll('a')]
        urls = [base_url+i if i.startswith('/') else i for i in urls]
        urls = [i for i in urls if i!=base_url and i!=base_url+'/']
        return urls

    def get_linkedin_company_url_listinput(self,urls,base_url):
        '''
        :param urls:
        :return:
        '''
        for url in urls:
            if re.search('linkedin.com/company',url): #if a linkedin company link found, return it
                return (url,100)
        #if the code comes here, it means no linkedin url present.
        #Now either we can look at other links like contact/about us page or do a ddg search
        #create two threads and do both in each one
        return self.get_linkedin_secondary_research(urls,base_url)

    def get_linkedin_secondary_research(self,urls,base_url):
        '''
        :param urls:
        :param base_url:
        :return:
        '''
        res_dic = {}
        event = threading.Event()
        t1 = threading.Thread(target=self.get_link_ddg_search, args=(base_url,event,res_dic,))
        t1.daemon = True
        t1.start()
        event.wait(timeout=self.time_out)
        if 'ddg_result' in res_dic:
            return res_dic['ddg_result']
        return ('',0)

    def get_link_ddg_search(self,base_url,event,res_dic):
        '''
        :param base_url:
        :return:
        '''
        search_query = base_url+' linkedin'
        search_res = self.ddg_crawler.fetch_results(search_query)
        conf = 95
        for dic1 in search_res:
            url,text = dic1['url'],dic1['text']
            if re.search('linkedin.com/company',url):
                res_dic['ddg_result'] = (url,conf)
                break
            conf = min(conf-5,60)
        event.set()

class CompanyLinkedinURLExtractorMulti(object):
    def __init__(self):
        logging.basicConfig(filename='log_file.log', level=logging.INFO,format='%(asctime)s %(message)s')

    def get_linkedin_url_single(self):
        '''
        :param url:
        :return:
        '''
        link_extractor = CompanyLinkedinURLExtractorSingle()
        def get_output(link_extractor,url,res_1,event):
            start_time = time.time()
            try:
                res_1['result'] = link_extractor.get_linkedin_url(url)
            except:
                logging.exception('Error while trying to fetch for url:{0}'.format(url))
            logging.info('completed run for url:{0} in {1} seconds, result: {2}'.format(url,time.time()-start_time,res_1))
            event.set()
        while self.run_queue:
            key,url = self.in_queue.get()
            res_1 = {}
            logging.info('Trying to find linkedin url for : {0},{1}, thread:{2}'.format(key,url,threading.currentThread()))
            event = threading.Event()
            t1 = threading.Thread(target=get_output, args=(link_extractor,url,res_1,event,))
            t1.daemon = True
            t1.start()
            event.wait(timeout=self.time_out)
            if 'result' in res_1:
                res = res_1['result']
                self.out_queue.put((key,res))
            logging.info('completed for : {0},{1}, thread:{2},res_1:{3}'.format(key,url,threading.currentThread(),res_1))
            self.in_queue.task_done()


    def get_outdic(self):
        '''
        :return:dictionary of key:linkedin_url fetched pair
        '''
        out_dic = {}
        while not self.out_queue.empty():
            key,linkedin_url = self.out_queue.get()
            logging.info('fetched from out_queue: {0},{1}'.format(key,linkedin_url))
            out_dic[key] = linkedin_url
        for key in self.url_dict.keys():
            if key not in out_dic:
                logging.info('key not found for key:{0}, giving default'.format(key))
                out_dic[key] = ('',0)
        return out_dic

    def get_linkedin_url_multi(self,url_dict,n_threads=5,time_out=30):
        '''
        :param url_dict: {key1:url1,key2:url2,...}
        :return:
        '''
        self.url_dict = url_dict
        self.time_out = time_out
        self.run_queue = True
        self.in_queue = Queue(maxsize=0)
        self.out_queue = Queue(maxsize=0)
        logging.info('threads started')
        for i in range(n_threads):
            worker = threading.Thread(target=self.get_linkedin_url_single)
            worker.setDaemon(True)
            worker.start()
        logging.info('putting urls into input queue')
        for i in url_dict:
            self.in_queue.put((i,url_dict[i]))
        start_time = time.time()
        self.in_queue.join()
        self.run_queue = False
        return self.get_outdic()