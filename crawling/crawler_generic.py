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
import gc

import crawler
from crawler import linkedin_url_clean_regex
# linkedin_url_clean_regex=r'\?trk=pub-pbmap|\?trk=prof-samename-picture|\?trk=extra_biz_viewers_viewed|\?trk=biz_employee_pub'

class LinkedinCrawlerThread(object):
    def __init__(self):
        '''
        '''
        pass

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

    def get_finished_links_single_line(self,line,var_name='Linkedin URL'):
        '''
        :param line:
        :return:
        '''
        tmp = eval(line)
        return tmp[var_name]

    def get_finished_links(self,var_name='Linkedin URL',*args):
        '''
        :param var_name: dictionary key for the finished url
        :param args: list of file names
        :return:
        '''
        link_list = []
        for f_name in args:
            with open(f_name,'r') as f:
                for line in f:
                    try:
                        link_list.append(self.get_finished_links_single_line(line,var_name))
                    except:
                        continue
        return list(set(link_list))

    def get_connected_urls_single_line(self,line,field_list,page_field,
                                       remove_regex=linkedin_url_clean_regex):
        '''
        :param line:
        :param field_list:
        :param page_field:
        :param remove_regex:
        :return:
        '''
        out_list = []
        tmp = eval(line)
        for field in field_list:
            if field in tmp:
                for det in tmp[field]:
                    out_list.append(re.sub(remove_regex,'',det[page_field]))
        return out_list

    def get_connected_urls(self,name_list,field_list,page_field,
                           remove_regex = linkedin_url_clean_regex):
        '''get all connected urls
        :param name_list:
        :param page_field: in the dictionary, this is the key for linkedin url for the connected pages
        :param field_list: Names of the dictionary keys for which the connected urls are present. For companies, it is
                'Also Viewed Companies', for people it is 'Related People','Same Name People'
        :param remove_regex: these are tracking extensions in the extracted urls, which can be removed
        :return: list of connected urls
        '''
        out_list = []
        for name in name_list:
            with open(name,'r') as f_in:
                for line in f_in:
                    out_list.extend(self.get_connected_urls_single_line(line,field_list,page_field,remove_regex))
                    # tmp = eval(line)
                    # for field in field_list:
                    #     if field in tmp:
                    #         for det in tmp[field]:
                    #             out_list.append(re.sub(remove_regex,'',det[page_field]))
        out_list = list(set(out_list))
        return out_list

    def run_organization_crawler_single(self,res_file=None,log_file=None,crawled_loc='crawled_res/',browser='Firefox',
                                        visible=False,proxy=True,url_file='company_urls_to_crawl_18April.pkl',
                                        n_threads=6,use_tor=False,use_db=False,limit_no=2000,urls_to_crawl_table='linkedin_company_urls_to_crawl',
            urls_to_crawl_priority='linkedin_company_urls_to_crawl_priority',base_table='linkedin_people_base',
            urls_to_crawl_table_people = 'linkedin_people_urls_to_crawl',finished_urls_table_company = 'linkedin_company_finished_urls',
            finished_urls_table_people = 'linkedin_people_finished_urls'):
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
            finished_urls_table_people = finished_urls_table_people)

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
                res_file = crawled_loc+'company_crawled_'+re.sub(' ','_',str(datetime.datetime.now())[:-13])+'.txt'
                log_file = crawled_loc+'logs/company_crawling'+re.sub(' ','_',str(datetime.datetime.now())[:-13])+'.log'
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
                                        visible=False,proxy=True,url_file='people_urls_to_crawl_18April.pkl',
                                        n_threads=6,use_tor=False,use_db=False,limit_no=2000,urls_to_crawl_table='linkedin_people_urls_to_crawl',
            urls_to_crawl_priority='linkedin_people_urls_to_crawl_priority',base_table='linkedin_people_base',
            urls_to_crawl_table_company = 'linkedin_company_urls_to_crawl',finished_urls_table_company = 'linkedin_company_finished_urls',
            finished_urls_table_people = 'linkedin_people_finished_urls'):
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
            finished_urls_table_people = finished_urls_table_people)

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
                res_file = crawled_loc+'people_crawled_'+re.sub(' ','_',str(datetime.datetime.now())[:-13])+'.txt'
                log_file = crawled_loc+'logs/people_crawling'+re.sub(' ','_',str(datetime.datetime.now())[:-13])+'.log'
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

    def get_single_field(self,line,field_name_list):
        ''' get a single parameter
        :param line:
        :param field_name_list:
        :return:
        '''
        pass

    def gen_url_lists_single(self,file_names,field_list,page_field,var_name = 'Linkedin URL',
                                       remove_regex=linkedin_url_clean_regex,
                                       ):
        '''
        :param file_names: names of files
        :param field_list: the fields which contain connected urls
        :param page_field: which url to extract
        :param var_name: key name for id (here linkedin url for each row)
        :param remove_regex:
        :return:
        '''
        connected_urls,finished_urls = [],[]
        for f_name in file_names:
            with open(f_name,'r') as f_in:
                for line in f_in:
                    # line = eval(line)
                    connected_urls.extend(self.get_connected_urls_single_line(line,field_list,page_field,remove_regex))
                    finished_urls.append(self.get_finished_links_single_line(line,var_name))
        return connected_urls,finished_urls

    def gen_url_lists(self,crawled_loc = 'crawled_res/',limit_no=30000,ignore_urls=[],add_urls_people=[],add_urls_company=[]):
        '''
        :param crawled_loc:
        :param limit_no:
        :param ignore_urls: these urls need to be ignored eg: to be used by server
        :param add_urls_company : add separate list of urls here, they will be tried to crawl first. otherwise taken from default
        :param add_urls_people : same as above, for people urls
        :return:
        '''
        crawled_files_company = self.get_files_in_dir(crawled_loc,match_regex='^company.+\.txt$')
        if crawled_files_company:
            crawled_files_company.sort()
            # extracting also viewed company details
            connected_urls_company,finished_urls_company = self.gen_url_lists_single(file_names=[crawled_files_company[-1]],
                                                            field_list=['Also Viewed Companies'],
                                                            page_field='company_linkedin_url')
            # extracting employee details
            people_urls_company,_ = self.gen_url_lists_single(file_names=[crawled_files_company[-1]],
                                                            field_list=['Employee Details'],
                                                            page_field='linkedin_url')
        else:
            connected_urls_company,finished_urls_company,people_urls_company = [],[],[]
        crawled_files_people = self.get_files_in_dir(crawled_loc,match_regex='^people.+\.txt$')
        if crawled_files_people:
            crawled_files_people.sort()
            # extracting related people and same name people
            connected_urls_people,finished_urls_people = self.gen_url_lists_single(file_names=[crawled_files_people[-1]],
                                                            field_list=['Related People','Same Name People'],
                                                            page_field='Linkedin Page')
            # extracting company details for people
            company_urls_people,_ = self.gen_url_lists_single(file_names=[crawled_files_people[-1]]
                                                              ,field_list=['Experience'],page_field='Company Linkedin')
        else:
            connected_urls_people,finished_urls_people,company_urls_people = [],[],[]
        connected_urls_people = connected_urls_people + people_urls_company
        connected_urls_company = connected_urls_company+company_urls_people
        del people_urls_company,company_urls_people
        finished_urls = list(set(finished_urls_company+finished_urls_people))
        for f_name in ignore_urls:
            with open(f_name,'r') as f:
                finished_urls.extend(pickle.load(f))
        del finished_urls_people,finished_urls_company
        gc.collect()
        finished_urls = set(finished_urls)
        with open('finished_urls.pkl','r') as f:
            finished_urls_old = pickle.load(f)
        finished_urls = set(finished_urls_old).union(set(finished_urls))
        del finished_urls_old
        with open('finished_urls.pkl','w') as f:
            pickle.dump(list(finished_urls),f)
        #fetching current remaining urls list
        with open('company_remaining_urls.pkl','r') as f:
            connected_urls_company_old = pickle.load(f)
        with open('people_remaining_urls.pkl','r') as f:
            connected_urls_people_old = pickle.load(f)
        connected_urls_company = set(connected_urls_company_old).union(set(connected_urls_company))
        connected_urls_people = set(connected_urls_people_old).union(set(connected_urls_people))
        del connected_urls_company_old,connected_urls_people_old
        gc.collect()
        connected_urls_company = list(connected_urls_company-finished_urls)
        connected_urls_people = list(connected_urls_people-finished_urls)
        shuffle(connected_urls_people)
        shuffle(connected_urls_company)
        for f_name in add_urls_people:
            with open(f_name,'r') as f:
                tmp = pickle.load(f)
                tmp = list(set(tmp)-finished_urls)
                connected_urls_people.extend(tmp)
        for f_name in add_urls_company:
            with open(f_name,'r') as f:
                tmp = pickle.load(f)
                tmp = list(set(tmp)-finished_urls)
                connected_urls_company.extend(tmp)
                del tmp
        #no shuffling here because urls in add_urls is given more preference
        with open('company_remaining_urls.pkl','w') as f:
            pickle.dump(list(set(connected_urls_company[:-limit_no])-finished_urls),f)
        with open('company_urls_to_crawl.pkl','w') as f:
            pickle.dump(list(set(connected_urls_company[-limit_no:])-finished_urls),f)
        with open('people_remaining_urls.pkl','w') as f:
            pickle.dump(list(set(connected_urls_people[:-limit_no])-finished_urls),f)
        with open('people_urls_to_crawl.pkl','w') as f:
            pickle.dump(list(set(connected_urls_people[-limit_no:])-finished_urls),f)
        del connected_urls_company,connected_urls_people,finished_urls
        gc.collect()

    def run_both_single(self,crawled_loc='crawled_res/',browser = 'Firefox',visible=False,
                                 proxy=True,limit_no=30000,n_threads=6,use_tor=False,use_db=False,
                                 urls_to_crawl_people='linkedin_people_urls_to_crawl',
            urls_to_crawl_people_priority='linkedin_people_urls_to_crawl_priority',people_base_table='linkedin_people_base',
            urls_to_crawl_company = 'linkedin_company_urls_to_crawl',urls_to_crawl_company_priority='linkedin_company_urls_to_crawl_priority',
                                 company_base_table='linkedin_company_base',finished_urls_table_company = 'linkedin_company_finished_urls',
            finished_urls_table_people = 'linkedin_people_finished_urls'):
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
                                                finished_urls_table_company ,finished_urls_table_people ))
        worker_company = multiprocessing.Process(target=self.run_organization_crawler_single,
                                                 args=(None,None,crawled_loc,browser,visible,proxy,
                                                'company_urls_to_crawl.pkl',n_threads,use_tor,use_db,limit_no,
                                                 urls_to_crawl_company,urls_to_crawl_company_priority,company_base_table,urls_to_crawl_people,
                                                 finished_urls_table_company ,finished_urls_table_people ))
        gc.collect()
        worker_people.daemon = True
        worker_company.daemon = True
        worker_people.start()
        worker_company.start()
        gc.collect()
        worker_people.join()
        worker_company.join(timeout=600)
        if worker_people.is_alive():
            worker_people.terminate()
        if worker_company.is_alive():
            worker_company.terminate()
        time.sleep(10)

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
        cc.run_both_single(limit_no=limit_no,n_threads=n_threads,use_tor=use_tor,use_db=True,
            urls_to_crawl_people=urls_to_crawl_people,urls_to_crawl_people_priority=urls_to_crawl_people_priority,
            people_base_table=people_base_table,urls_to_crawl_company = urls_to_crawl_company,
            urls_to_crawl_company_priority=urls_to_crawl_company_priority,company_base_table=company_base_table,
            finished_urls_table_company=finished_urls_table_company,finished_urls_table_people=finished_urls_table_people)


