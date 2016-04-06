__author__ = 'ideas2it'

"""
File : linkedin_profile_crawler.py
Created On: 14-Mar-2016
Author: ideas2it
"""
import re
from bs_crawl import BeautifulsoupCrawl
# from bs4 import BeautifulSoup

class CrunchbaseProfileCrawler(object):
    '''Crawl a linkedin profie page
    '''
    def __init__(self):
        '''
        :return:
        '''
        pass

    def fetch_details_urlinput(self,url):
        '''
        :param url:
        :param outs_needed: list of parameters need to be fetched. if empty, all are fetched
        :return: dictionary with the fetched details
        '''
        soup = BeautifulsoupCrawl.single_wp(url)
        outs = self.fetch_details_soupinput(soup)
        return outs

    def fetch_details_soupinput(self,soup):
        '''
        :param soup:
        :return: dictionary with the fetched details
        '''
        outs = {}
        outs['Name'] = self.get_name(soup)
        outs['CompanyCrunchbaseLink'] = self.get_company_cruchbase_page(soup)
        other_dets = self.get_other_details(soup)
        for key in other_dets:
            outs[key] = other_dets[key]
        return outs

    def get_name(self,soup):
        '''
        :param soup:
        :return:
        '''
        try:
            return soup.find('div',{'class':'base'}).find('h1',{'id':'profile_header_heading'}).text
        except:
            return ''

    def get_company_cruchbase_page(self,soup):
        '''
        :param soup:
        :return:
        '''
        try:
            tmp = soup.find('div',{'class':'definition-list-container'}).find('div',{'class':'overview-stats'})\
                .find('dd').find('a',{'class':'follow_card'})['href']
            return 'https://www.crunchbase.com'+tmp
        except:
            return ''


    def get_other_details(self,soup):
        ''' details in tabular format
        :param soup:
        :return:
        '''
        try:
            tags = soup.find('div',{'class':'definition-list-container'}).findAll('dt')
            values = soup.find('div',{'class':'definition-list-container'}).findAll('dd')
            out_dic = {}
            assert len(tags) == len(values)
            for ind in range(len(tags)):
                tag_name = re.sub(':','',tags[ind].text)
                if tag_name == 'Social ':
                    link_tags = values[ind].findAll('a')
                    link_list = [tagg['href'] for tagg in link_tags]
                    out_dic[tag_name] = link_list
                else:
                    out_dic[tag_name] = values[ind].text
            return out_dic
        except:
            return {}