"""
File : duckduckgo_crawler.py
Created On: 07-Mar-2016
Author: ideas2it
"""
import pdb
import urllib
from bs_crawl import BeautifulsoupCrawl

class DuckduckgoCrawler(object):
    '''
    '''
    def __init__(self):
        pass

    def fetch_results(self,query):
        '''
        :param query: search query. eg: prime minister of india
        :return:
        '''
        url = 'https://duckduckgo.com/html/?q='+urllib.quote_plus(query)
        soup = BeautifulsoupCrawl.single_wp(url)
        # pdb.set_trace()
        # res_links = soup.find('div',{'id':'links','class':'results'}).findAll('div',{'class':'result results_links results_links_deep web-result '})
        res_links = soup.findAll('div',{'class':'result results_links results_links_deep web-result '})
        res_list = []
        for link_soup in res_links:
            res_list.append({'url':link_soup.find('a',{'class':'result__a','rel':'nofollow'})['href']
                ,'text':link_soup.find('a',{'class':'result__snippet'}).text})
        return res_list