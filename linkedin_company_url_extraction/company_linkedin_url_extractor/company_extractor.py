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
        :param time_out:
        :return:
        '''
        logging.info('get_linkedin_url url:{}'.format(company_url))
        soup = self.crawler.single_wp(company_url,timeout=time_out)
        if str(soup):
            urls = self.get_urls_soupinput(soup,company_url)
            linkedin_url,confidence = self.get_linkedin_company_url_listinput(urls,company_url)
            if confidence>0 and linkedin_url:
                return linkedin_url,confidence
        else: #else try from ddg. if could not find from main page also, try ddg
            pass
        search_query = company_url+' linkedin'
        search_res = self.ddg_crawler.fetch_results(search_query,timeout=time_out)
        conf = 95
        for dic1 in search_res:
            res_url,text = dic1['url'],dic1['text']
            if re.search('linkedin.com/company',res_url):
                return res_url,conf
            conf = min(conf-5,60)
        return '',0

    def get_linkedin_url_threaded(self,company_url,time_out=30):
        '''threaded implementation
        :param company_url:
        :return:
        '''
        logging.info('get_linkedin_url url:{}'.format(company_url))
        self.time_out = time_out
        res_dic = {}
        event = threading.Event()
        t1 = threading.Thread(target=self.get_urls_urlinput, args=(company_url,res_dic,event,))
        t1.daemon = True
        t1.start()
        event.wait(timeout=self.time_out)
        if 'Url direct result' in res_dic:
            if res_dic['Url direct result'][1] > 0:
                return res_dic['Url direct result']
        logging.info('get_linkedin_url could not find from main page. trying secondary, url:{}'.format(company_url))
        #if not obtained from url, try secondary
        event = threading.Event()
        t1 = threading.Thread(target=self.get_link_ddg_search, args=(company_url,event,res_dic,))
        t1.daemon = True
        t1.start()
        event.wait(timeout=self.time_out)
        if 'Url direct result' in res_dic:
            if res_dic['Url direct result'][1] > 0:
                return res_dic['Url direct result']
        if 'ddg_result' in res_dic:
            return res_dic['ddg_result']
        logging.info('get_linkedin_url could not find by secondary. url:{}, res_dic:{}'.format(company_url,res_dic))
        return ('',0)

    def get_urls_urlinput(self,base_url,res_dic,event):
        '''
        :param base_url:
        :return:
        '''
        soup = self.crawler.single_wp(base_url)
        urls = self.get_urls_soupinput(soup,base_url)
        res_dic['Url direct result'] = self.get_linkedin_company_url_listinput(urls,base_url)
        event.set()

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
                # logging.exception('Error while trying to fetch link for url:{0},link:{1}'.format(base_url,i))
                continue
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
        # logging.info('couldnt find link in page. trying secondary sources. url:{}'.format(base_url))
        # return self.get_linkedin_secondary_research(urls,base_url)
        return '',0

    def get_linkedin_secondary_research(self,base_url):
        '''
        :param urls: list of urls to follow. not using currently
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
        # def get_output(link_extractor,url,res_1,event):
        #     start_time = time.time()
        #     try:
        #         res_1['result'] = link_extractor.get_linkedin_url(url)
        #     except:
        #         logging.info('Error while trying to fetch for url:{0}'.format(url))
        #     logging.info('completed run for url:{0} in {1} seconds, result: {2}'.format(url,time.time()-start_time,res_1))
        #     event.set()
        while self.run_queue:
            key,url = self.in_queue.get()
            logging.info('Trying to find linkedin url for : {0},{1}, thread:{2}'.format(key,url,threading.currentThread()))
            res = link_extractor.get_linkedin_url(url,time_out=self.time_out)
            self.out_queue.put((key,res))
            logging.info('completed for : {0},{1}, res:{2},thread:{3}'.format(key,url,res,threading.currentThread()))
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

    def get_linkedin_url_multi(self,url_dict,n_threads=2,time_out=30):
        '''
        :param url_dict: {key1:url1,key2:url2,...}
        :return:
        '''
        self.url_dict = url_dict
        n_threads = min(n_threads,len(self.url_dict))
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