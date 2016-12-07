__author__ = 'joswin'

from bs_crawl import BeautifulsoupCrawl
from selenium_crawl import SeleniumParser
from duckduckgo_crawler import DuckduckgoCrawler
from urlparse import urljoin
from random import shuffle,choice,randint

import re
import logging
import threading
import logging
import time
from Queue import Queue

from constants import company_common_reg,user_agents

class CompanyLinkedinURLExtractorSingle(object):
    '''Find url for a single company/this will detect people linkedin urls also
    '''
    def __init__(self,visible=False):
        # self.crawler = BeautifulsoupCrawl()
        self.init_crawler(visible)
        # self.search_string = r'linkedin.com/company/|linkedin.com/companies/|linkedin.com/pub/|linkedin.com/in/' #earlier linkedin.com/company
        self.search_string_website = r'linkedin.com/company/|linkedin.com/companies/|linkedin.com/pub/|linkedin.com/in/'
        self.search_string_ddg = r'linkedin.com/company/|linkedin.com/companies/'
        self.search_string_people = r'linkedin.com/pub/|linkedin.com/in/'
        # self.search_string_ddg = r'linkedin.com/company/|linkedin.com/companies/|linkedin.com/pub/(?!dir/)|linkedin.com/in/'

    def init_crawler(self,visible=False):
        ''' '''
        # self.crawler = SeleniumParser(page_load_timeout=50, visible=visible)
        self.crawler = BeautifulsoupCrawl()
        self.ddg_crawler = DuckduckgoCrawler(visible=visible,headers=choice(user_agents))

    def exit_crawler(self):
        ''' '''
        self.crawler.exit()

    def get_linkedin_url(self,inp_tuple,time_out=30):
        '''
        :param inp_tuple:(company_url,additional_text)
        :param time_out:
        :return:company_url,confidence,other_urls_list
        other_urls_list are urls of people.
        '''
        logging.info('company extraction: trying for company : {}'.format(inp_tuple))
        company_url,additional_text = inp_tuple
        company_url = re.sub(r'\?+',' ',company_url.encode('ascii','replace'))
        additional_text = re.sub(r'\?+',' ',additional_text.encode('ascii','replace'))
        # logging.info('get_linkedin_url url:{}'.format(company_url))
        # if re.search('http',company_url) or re.search('www',company_url) or re.search('\.co',company_url):
        #     if not re.search('www',company_url) and not re.search('http',company_url):
        #         company_url = 'http://www.'+company_url
        #     elif re.search('www',company_url) and not re.search('http',company_url):
        #         company_url = 'http://'+company_url
        #     # soup = self.crawler.single_wp(company_url,timeout=time_out)
        #     try:
        #         soup = self.crawler.get_soup(company_url)
        #         if str(soup):
        #             urls = self.get_urls_soupinput(soup,company_url)
        #             linkedin_url,confidence = self.get_linkedin_company_url_listinput(urls,company_url)
        #             if confidence>0 and linkedin_url:
        #                 return linkedin_url,confidence,[]
        #         else: #else try from ddg. if could not find from main page also, try ddg
        #             pass
        #     except:
        #         logging.exception('get_linkedin_url: error happened while processing company url:{}.'
        #                           ' try duckduckgo'.format(company_url))
        res,conf,people_urls = self.get_linkedin_url_ddg(company_url,additional_text)
        if not res or conf == 0:
            res,conf,people_urls = self.get_linkedin_url_ddg(company_url,'') #if additional_text (company name)
                                                                                # could not find, try with only website
        if res and conf>0 :
            # first check if input is not linkedin and url found is for linkedin
            if re.search('/company/1337|/company/linkedin|/company-beta/1337|/company/facebook|github|'\
                         '/company/google|/company/twitter|/company/yahoo|/company/myspace|/company/yelp|'\
                         '/company/youtube|/company/vimeo|/company/instagram',res,re.IGNORECASE) and \
                not re.search('linkedin|facebook|google|twitter|yahoo|myspace|yelp|youtube|vimeo|instagram|github',
                              additional_text,re.IGNORECASE) :
                logging.info('Could not find linkedin url for company : {} ,name: {}'.format(company_url,additional_text))
                return '',0,[]
            logging.info(u'Found linkedin url for domain: {} ,name: {} ,url: {}'.format(company_url,additional_text,res))
            return res,conf,people_urls
        else:
            logging.info(u'Could not find linkedin url for company : {} ,name: {}'.format(company_url,additional_text))
            return '',0,[]

    def get_linkedin_url_ddg(self,company_text,additional_text=''):
        '''
        :param company_text: this is the website of the company
        :param additional_text : the name of the company
        :return:
        '''
        # need to make the results better. from now onwards, company_text will be website, and additional_text will be
        # company name. search only using company name
        # if company_text:
        #     search_query = company_text+' linkedin'
        #     search_res = self.ddg_crawler.fetch_results(search_query)
        #     conf = 95
        #     res_list = []
        #     for dic1 in search_res:
        #         res_url,text = dic1['url'],dic1['text']
        #         if re.search(self.search_string_ddg,res_url):
        #             res_list.append((res_url,text,conf))
        #         conf = min(conf-5,60)
        #     final_res,final_conf = self.get_best_res_from_ddg_results(res_list,company_text,'')
        #     if final_conf >0 and final_res:
        #         return final_res,final_conf
        time.sleep(randint(5,10))
        if not additional_text:
            additional_text = company_text
            additional_text = re.sub(r'http://(app\.)?|https://(app\.)?|www\.(app\.)?','',additional_text)
            # additional_text = re.split(r'\.co.{0,4}$|\.[a-zA-Z/]+$|\.gov|\.in$|\.us$',additional_text)[0]\
            additional_text = re.split('\.',re.split('/',re.sub(r'http://|https://|www\.','',additional_text))[0])[0]
            logging.info('url cleaning from {} to {}'.format(company_text,additional_text))
        if additional_text:
            additional_text = re.sub(' +',' ',additional_text)
            additional_text = company_common_reg.sub(' ',additional_text)
            additional_text = re.sub(' +',' ',additional_text)
            additional_text = re.sub(r'http://|https://|www\.','',additional_text)
            search_query = additional_text+' linkedin'
            search_res = self.ddg_crawler.fetch_results(search_query) # go to ddg and get all results in list
            search_res = search_res[:min(len(search_res),7)]
            time.sleep(4)
            conf = 90
            res_list,people_list = [],[]
            for dic1 in search_res:
                res_url,text,url_text = dic1['url'],dic1['text'],dic1['url_text']
                if re.search(self.search_string_ddg,res_url):
                    res_list.append((res_url,text,url_text,conf))
                if re.search(self.search_string_people,res_url):
                    # if not re.search(r'at linkedin',text,re.IGNORECASE): this works in google search, not in ddg
                    people_list.append(res_url)
                conf = min(conf-5,60)
            final_res,final_conf = self.get_best_res_from_ddg_results(res_list,additional_text,'')
            if final_conf >0 and final_res:
                return final_res,final_conf,people_list
        return '',0,[]

    def get_best_res_from_ddg_results(self,res_list,company_text,additional_text=''):
        ''' match the results to get best result from ddg results. Will use string matching here
        :param res_list:
        :param company_text
        :return:
        '''
        if re.search('http',company_text) or re.search('www',company_text):
            company_text = re.sub('https|http|www|\.com',' ',company_text)
            company_text = re.sub('[^a-zA-Z0-9]',' ',company_text)
        company_text = company_text.lower()
        company_text = re.sub(' +',' ',company_text)
        company_text = company_common_reg.sub(' ',company_text,re.IGNORECASE)
        company_text = re.sub(' +',' ',company_text)
        company_text = company_text.strip()
        # company_text_wrds = company_text.split(' ')
        # company_text_wrds = [i for i in company_text_wrds if i]
        # res_list_new = []
        # max_count = 0
        regex_to_sub = ' |[^a-zA-Z0-9]' #remove spaces and special characters
        company_text_without_space = re.sub(regex_to_sub,'',company_text)
        for url,text,url_text,conf in res_list:
            # url_1 = re.sub(' +',' ',re.sub(r'[^a-zA-Z0-9]',' ',re.sub('https|http|www|\.com',' ',url)).lower())
            # url_1 = company_common_reg.sub(' ',url_1)
            # url_1 = re.sub(' +',' ',url_1)
            text_1 = re.sub(' +',' ',text).lower()
            text_1 = company_common_reg.sub(' ',text_1)
            text_2 = re.sub(regex_to_sub,'',text_1)
            url_text_1 = re.sub(' +',' ',url_text).lower()
            url_text_1 = company_common_reg.sub(' ',url_text_1)
            url_text_1 = re.sub(regex_to_sub,'',url_text_1)
            if  re.search(company_text_without_space,url_text_1): #re.search(company_text_without_space,text_2) or
                # res_list_new.append((url,conf,100)) #add 100 as match for text
                return url,90 #return url with confidence 100
        return '',0

class CompanyLinkedinURLExtractorMulti(object):
    def __init__(self,visible=False):
        # logging.basicConfig(filename='log_file.log', level=logging.INFO,format='%(asctime)s %(message)s')
        self.visible = visible

    def get_linkedin_url_single(self,link_extractor):
        '''
        :param url:
        :return:
        '''
        # def get_output(link_extractor,url,res_1,event):
        #     start_time = time.time()
        #     try:
        #         res_1['result'] = link_extractor.get_linkedin_url(url)
        #     except:
        #         logging.info('Error while trying to fetch for url:{0}'.format(url))
        #     logging.info('completed run for url:{0} in {1} seconds, result: {2}'.format(url,time.time()-start_time,res_1))
        #     event.set()
        while self.run_queue:
            try:
                key,inp_tuple = self.in_queue.get(timeout=10)
            except:
                logging.exception('get_linkedin_url_single: in_queue empty')
                break
            logging.info('get_linkedin_url_single: trying for key:{},inp_tuple:{}'.format(key,inp_tuple))
            # logging.info('Trying to find linkedin url for : {0},{1}, thread:{2}'.format(key,inp_tuple,threading.currentThread()))
            try:
                res = link_extractor.get_linkedin_url(inp_tuple,time_out=self.time_out)
                #res is a tuple (company_url,confidence,other_urls_list)
                # if res:
                self.out_queue.put((key,res))
                # logging.info('completed for : {0},{1}, res:{2},thread:{3}'.format(key,inp_tuple,res,threading.currentThread()))
            except:
                logging.exception('get_linkedin_url_single:exception while trying for key:{},inp_tuple:{}'.format(key,inp_tuple))
                try:
                    link_extractor.exit_crawler()
                except:
                    pass
                link_extractor.init_crawler(self.visible)
                pass
            self.in_queue.task_done()

    def get_outdic(self):
        '''not used
        :return:dictionary of key:linkedin_url fetched pair
        '''
        out_dic = {}
        while not self.out_queue.empty():
            key,linkedin_url = self.out_queue.get() #what we get here is not linkedin url. it is a tuple
            #                                         (company_url,confidence,other_urls_list)
            # logging.info('fetched from out_queue: {0},{1}'.format(key,linkedin_url))
            out_dic[key] = linkedin_url
        for key in self.url_dict.keys():
            if key not in out_dic:
                # logging.info('could not find linkedin ur for key:{0}'.format(key))
                out_dic[key] = ('',0)
        return out_dic

    def yield_outs(self):
        ''' not used
        :return:
        '''
        while not self.out_queue.empty():
            key,linkedin_url = self.out_queue.get()
            yield key,linkedin_url

    def get_linkedin_url_multi(self,url_dict,n_threads=2,time_out=30):
        '''
        :param url_dict: {key1:(url1,company_name1),key2:(url2,company_name2),...}
             key here is list_items_id in list_items table
        :return:
        '''
        self.url_dict = url_dict
        n_threads = min(n_threads,len(self.url_dict))
        self.time_out = time_out
        self.run_queue = True
        self.in_queue = Queue(maxsize=0)
        self.out_queue = Queue(maxsize=0)
        # logging.info('threads started')
        workers = []
        worker_link_extractors = []
        for i in range(n_threads):
            link_extractor = CompanyLinkedinURLExtractorSingle(visible=self.visible)
            worker = threading.Thread(target=self.get_linkedin_url_single,args=(link_extractor,))
            worker.setDaemon(True)
            workers.append(worker)
            worker_link_extractors.append(link_extractor)
            worker.start()
        # logging.info('putting urls into input queue')
        url_dict_keys = url_dict.keys()
        shuffle(url_dict_keys)
        for i in url_dict_keys:
            self.in_queue.put((i,url_dict[i]))
        logging.info('company extraction: 120 second wait started ')
        time.sleep(120)
        logging.info('company extraction: 120 second wait ended ')
        # start yielding results
        found_list = []
        logging.info('company extraction: looking at out_queue for urls')
        while not self.in_queue.empty():
            try:
                key,res = self.out_queue.get(timeout=240)
                # res is a tuple (company_url,confidence,other_urls_list)
            except:
                logging.exception('company extraction: error while getting from out queue')
                # break # this is not needed???
                continue
            linkedin_url,conf,people_urls = res[0],res[1],res[2]
            # logging.info('company extraction: url extracted :{}, conf:{}'.format(linkedin_url,conf))
            yield key,linkedin_url,conf,people_urls # yield key, url and flag_found(1). if not found return 0
            found_list.append(key)
            # time.sleep(1)
        logging.info('company extraction: out_queue is empty ')
        self.in_queue.join()
        self.run_queue = False
        for worker in workers:
            worker.join(timeout=1)
        for link_extractor in worker_link_extractors:
            link_extractor.crawler.browser.quit()
            # link_extractor.ddg_crawler.crawler.browser.quit()
        # try to yield from out_queue again if anything is remaining in the queue
        logging.info('company extraction: trying to fetch from out queue again')
        while not self.out_queue.empty() or not self.in_queue.empty():
            key,res = self.out_queue.get()
            try:
                key,res = self.out_queue.get(timeout=60) # res is a tuple (company_url,confidence,other_urls_list)
            except:
                break
            linkedin_url,conf,people_urls = res[0],res[1],res[2]
            yield key,linkedin_url,conf,people_urls
            found_list.append(key)
            # time.sleep(30)
        logging.info('company extraction: out queue empty. send remaining as not able to find ')
        for key in self.url_dict.keys():
            if key not in found_list:
                # logging.info('could not find linkedin ur for key:{0}'.format(key))
                yield key,'',0,[]
