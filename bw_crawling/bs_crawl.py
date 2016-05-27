"""
File : bs_crawl.py
Created On: 07-Mar-2016
Author: ideas2it
"""

from bs4 import BeautifulSoup

from urllib_crawl import UrllibCrawl
from url_cleaner import UrlCleaner
url_cleaner = UrlCleaner()

class BeautifulsoupCrawl:
    
    @staticmethod
    def single_wp(baseurl,headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:39.0) Gecko/20100101 Firefox/39.0'}):
        # parse the html using BeautifulSoup
        baseurl = url_cleaner.clean_url(baseurl)
        try:
            response=BeautifulSoup(UrllibCrawl.getResponse(baseurl,headers=headers))
        except:
            response = BeautifulSoup(UrllibCrawl.getResponse(baseurl,headers=False))
        
        # print("single_wp..........url...",response.prettify())
        
        return response

    @staticmethod
    def multiple_wp(baseurlList):
        print("url.............")
        
        return ""
