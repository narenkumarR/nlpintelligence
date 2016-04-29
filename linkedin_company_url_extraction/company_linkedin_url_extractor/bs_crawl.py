"""
File : bs_crawl.py
Created On: 07-Mar-2016
Author: ideas2it
"""

import threading
from bs4 import BeautifulSoup

from urllib_crawl import UrllibCrawl
from url_cleaner import UrlCleaner
url_cleaner = UrlCleaner()

class BeautifulsoupCrawl(object):

    def __init__(self,headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:39.0) Gecko/20100101 Firefox/39.0'}):
        self.headers = headers
    
    def single_wp(self,baseurl,headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:39.0) Gecko/20100101 Firefox/39.0'}):
        baseurl = url_cleaner.clean_url(baseurl)
        if headers:
            # parse the html using BeautifulSoup
            response=BeautifulSoup(UrllibCrawl.getResponse(baseurl,headers=headers))
        else:
            response = BeautifulSoup(UrllibCrawl.getResponse(baseurl,headers=self.headers))
        return response

    def multiple_wp(self,baseurlList,parallel=False,n_parallel = 4):
        '''use threads here
        :param baseurlList:
        :return:
        '''
        if parallel:
            pass
        else:
            out_list = []
            for url in baseurlList:
                out_list.append(self.single_wp(url))
            return out_list
