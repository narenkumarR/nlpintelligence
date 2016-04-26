__author__ = 'joswin'

import datetime
import time
import logging
import re
import os
import pickle
from random import shuffle
import multiprocessing
import sys

import crawler

class LinkedinCrawlerThread(object):
    def __init__(self):
        '''
        '''
        pass

    def get_finished_links(self,var_name='Linkedin URL',*args):
        link_list = []
        for f_name in args:
            with open(f_name,'r') as f:
                for line in f:
                    tmp = eval(line)
                    link_list.append(tmp[var_name])
        return list(set(link_list))

    def get_files_in_dir(self,dir_path='.',remove_regex = '',match_regex=''):
        ''' get matching files
        :param dir_path:
        :param remove_regex:
        :param match_regex:
        :return:
        '''
        files = os.listdir(dir_path)
        if dir_path == '.':
            dir_path = ''
        if remove_regex:
            files = [i for i in files if not re.search(remove_regex,i)]
        if match_regex:
            files = [i for i in files if re.search(match_regex,i)]
        files = [dir_path+i for i in files]
        return files

    def get_connected_urls(self,name_list,field_list,page_field='Linkedin Page',
                           remove_regex = r'\?trk=pub-pbmap|\?trk=prof-samename-picture|\?trk=extra_biz_viewers_viewed'):
        '''
        :param name_list:
        :return:
        '''
        out_list = []
        for name in name_list:
            with open(name,'r') as f_in:
                for line in f_in:
                    tmp = eval(line)
                    for field in field_list:
                        if field in tmp:
                            for det in tmp[field]:
                                out_list.append(re.sub(remove_regex,'',det[page_field]))
        out_list = list(set(out_list))
        return out_list

    def run_organization_crawler_single(self,res_file=None,log_file=None,crawled_loc='crawled_res/',browser='Firefox',
                                        visible=False,proxy=True,
                                 url_file='company_urls_to_crawl_18April.pkl',limit_no=60000,n_threads=6):
        '''
        :param res_file:
        :param log_file:
        :param crawled_loc:
        :param cc: the crawler object
        :param url_file:
        :param limit_no:
        :return:
        '''
        if res_file is None:
            res_file = crawled_loc+'company_crawled_'+str(datetime.date.today())+'.txt'
        if log_file is None:
            log_file = crawled_loc+'logs/company_crawling'+str(datetime.date.today())+'.log'
        crawled_files = self.get_files_in_dir(crawled_loc,match_regex='^company.+\.txt$')
        finished_urls = self.get_finished_links('Linkedin URL',*crawled_files)
        with open(url_file,'r') as f:
            urls = pickle.load(f)
        connected_urls = self.get_connected_urls(crawled_files,['Also Viewed Companies'],page_field='company_linkedin_url')
        urls = urls+connected_urls
        urls = list(set(urls)-set(finished_urls))
        shuffle(urls)
        urls = urls[:limit_no]
        cc = crawler.LinkedinCompanyCrawlerThread(browser,visible=visible,proxy=proxy)
        cc.run(urls,res_file,log_file,n_threads)

    def run_organization_crawler_auto(self,crawled_loc='crawled_res/',browser = 'Firefox',visible=False,
                                 proxy=True,url_file='company_urls_to_crawl_18April.pkl',limit_no=30000,n_threads=6,
                                 ):
        '''Need work
        :param crawled_loc:
        :param browser:
        :param visible:
        :param proxy:
        :param url_file:
        :param limit_no:
        :param n_threads:
        :return:
        '''
        cc = crawler.LinkedinCompanyCrawlerThread(browser,visible=visible,proxy=proxy)
        while True:
            try:
                print(datetime.datetime.now())
                res_file = crawled_loc+'company_crawled_'+str(datetime.date.today())+'.txt'
                log_file = crawled_loc+'logs/company_crawling'+str(datetime.date.today())+'.log'
                crawled_files = self.get_files_in_dir(crawled_loc,match_regex='^company.+\.txt$')
                finished_urls = self.get_finished_links('Linkedin URL',*crawled_files)
                print('finished urls:{}'.format(len(finished_urls)))
                with open(url_file,'r') as f:
                    urls = pickle.load(f)
                connected_urls = self.get_connected_urls(crawled_files,['Also Viewed Companies'],page_field='company_linkedin_url')
                print('connected urls:{}'.format(len(connected_urls)))
                urls = urls+connected_urls
                urls = list(set(urls)-set(finished_urls))
                shuffle(urls)
                urls = urls[:limit_no]
                print('urls to crawl:{}'.format(len(urls)))
                del connected_urls,finished_urls
                cc.run(urls,res_file,log_file,n_threads)
            except:
                logging.exception('Exception in main auto thread.wait for 10 seconds')
                print('error. wait for 10 seconds, then try again')
                time.sleep(10)
                continue

    def run_people_crawler_single(self,res_file=None,log_file=None,crawled_loc='crawled_res/',browser='Firefox',
                                        visible=False,proxy=True,
                                 url_file='people_urls_to_crawl_18April.pkl',limit_no=60000,n_threads=6):
        '''
        :param res_file:
        :param log_file:
        :param crawled_loc:
        :param cc: the crawler object
        :param url_file:
        :param limit_no:
        :return:
        '''
        if res_file is None:
            res_file = crawled_loc+'people_crawled_'+str(datetime.date.today())+'.txt'
        if log_file is None:
            log_file = crawled_loc+'logs/people_crawling'+str(datetime.date.today())+'.log'
        crawled_files = self.get_files_in_dir(crawled_loc,match_regex='^people.+\.txt$')
        finished_urls = self.get_finished_links('Linkedin URL',*crawled_files)
        with open(url_file,'r') as f:
            urls = pickle.load(f)
        connected_urls = self.get_connected_urls(crawled_files,['Related People','Same Name People'],
                                                 page_field='Linkedin Page')
        urls = urls+connected_urls
        urls = list(set(urls)-set(finished_urls))
        shuffle(urls)
        urls = urls[:limit_no]
        cc = crawler.LinkedinProfileCrawlerThread(browser,visible=visible,proxy=proxy)
        cc.run(urls,res_file,log_file,n_threads)

    def run_people_crawler_auto(self,crawled_loc='crawled_res/',browser = 'Firefox',visible=False,
                                 proxy=True,url_file='people_urls_to_crawl_18April.pkl',limit_no=30000,n_threads=6,
                                 ):
        '''Need work
        :param crawled_loc:
        :param browser:
        :param visible:
        :param proxy:
        :param url_file:
        :param limit_no:
        :param n_threads:
        :return:
        '''
        cc = crawler.LinkedinProfileCrawlerThread(browser,visible=visible,proxy=proxy)
        while True:
            try:
                print(datetime.datetime.now())
                res_file = crawled_loc+'people_crawled_'+str(datetime.date.today())+'.txt'
                log_file = crawled_loc+'logs/people_crawling'+str(datetime.date.today())+'.log'
                crawled_files = self.get_files_in_dir(crawled_loc,match_regex='^people.+\.txt$')
                finished_urls = self.get_finished_links('Linkedin URL',*crawled_files)
                print('finished urls:{}'.format(len(finished_urls)))
                with open(url_file,'r') as f:
                    urls = pickle.load(f)
                connected_urls = self.get_connected_urls(crawled_files,['Related People','Same Name People'],
                                                         page_field='Linkedin Page')
                print('connected urls:{}'.format(len(connected_urls)))
                urls = urls+connected_urls
                urls = list(set(urls)-set(finished_urls))
                shuffle(urls)
                urls = urls[:limit_no]
                del connected_urls,finished_urls
                print('urls to crawl:{}'.format(len(urls)))
                cc.run(urls,res_file,log_file,n_threads)
            except:
                logging.exception('Exception in main auto thread.wait for 10 seconds')
                print('error. wait for 10 seconds, then try again')
                time.sleep(10)
                continue

    def run_both_single(self,crawled_loc='crawled_res/',browser = 'Firefox',visible=False,
                                 proxy=True,limit_no=30000,n_threads=6):
        worker_people = multiprocessing.Process(target=self.run_people_crawler_single,args=(None,None,crawled_loc,browser,
                                        visible,proxy,'people_urls_to_crawl_18April.pkl',limit_no,n_threads))
        worker_company = multiprocessing.Process(target=self.run_organization_crawler_single,args=(None,None,crawled_loc,browser,
                                        visible,proxy,'people_urls_to_crawl_18April.pkl',limit_no,n_threads))
        worker_people.daemon = True
        worker_company.daemon = True
        worker_people.start()
        worker_company.start()
        worker_company.join()
        worker_people.join()
        time.sleep(10)

if __name__ == '__main__':
    cc = LinkedinCrawlerThread()
    if len(sys.argv)<3:
        cc.run_both_single()
    else:
        limit_no = sys.argv[1]
        limit_no = int(limit_no)
        n_threads = sys.argv[2]
        n_threads = int(n_threads)
        cc.run_both_single(limit_no=limit_no,n_threads=n_threads)
