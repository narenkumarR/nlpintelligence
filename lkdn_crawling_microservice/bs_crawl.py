"""
File : bs_crawl.py
Created On: 07-Mar-2016
Author: ideas2it
"""

from bs4 import BeautifulSoup

from urllib_crawl import UrllibCrawl

class BeautifulsoupCrawl:
    def __init__(self):
        pass

    def get_url(self,baseurl,headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:39.0) Gecko/20100101 Firefox/39.0'}):
        # parse the html using BeautifulSoup
        response=UrllibCrawl.getResponse(baseurl,headers=headers)

        # print("single_wp..........url...",response.prettify())

        return response

    def multiple_wp(self,baseurlList):
        print("url.............")

        return ""

    def get_soup(self,url,headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:39.0) Gecko/20100101 Firefox/39.0'}):
        soup = BeautifulSoup(self.get_url(url,headers))
        return soup

    def exit(self):
        pass
