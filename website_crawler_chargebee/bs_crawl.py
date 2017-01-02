"""
File : bs_crawl.py
Created On: 07-Mar-2016
Author: ideas2it
"""

from bs4 import BeautifulSoup

from urllib_crawl import UrllibCrawl

class BeautifulsoupCrawl(object):

    def __init__(self,page_load_timeout=120):
        self.page_load_timeout = page_load_timeout

    @staticmethod
    def single_wp(baseurl,headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:39.0) Gecko/20100101 Firefox/39.0'}):
        # parse the html using BeautifulSoup
        response=BeautifulSoup(UrllibCrawl.getResponse(baseurl,headers=headers,timeout=self.page_load_timeout))
        return response

    @staticmethod
    def multiple_wp(baseurlList):
        print("url.............")

        return ""

    @staticmethod
    def get_soup(url):
        response=BeautifulSoup(UrllibCrawl.getResponse(url))
        return response

    def exit(self):
        pass

    def start_browser(self,*args,**kwargs):
        pass
