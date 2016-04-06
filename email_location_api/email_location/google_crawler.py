"""
File : google_crawler.py
Created On: 07-Mar-2016
Author: ideas2it
"""
from bs_crawl import BeautifulsoupCrawl

class GoogleCrawler(object):
    '''
    '''
    def __init__(self):
        pass

    def fetch_results(self,query):
        '''
        :param query: search query. eg: prime minister of india
        :return:
        '''
        url = 'https://www.google.co.in/search?q='+'+'.join(query.split(' '))
        soup = BeautifulsoupCrawl.single_wp(url)
        res_links = soup.find('div',{'id':'rso'}).find('div',{'class':'srg'}).findAll('div',{'class':'g'})
        res_list = []
        for link_soup in res_links:
            res_url = link_soup.find('div',{'class':'s'}).find('cite').text
            try:
                res_text = link_soup.find('div',{'class':'s'}).find('div',{'class':'f slp'}).text 
            except:
                res_text = ''
            res_list.append({'url':res_url,'text':res_text})
        return res_list
