__author__ = 'joswin'

import re
import Levenshtein
import distance
from constants import company_stops_regex

from bs_crawl import BeautifulsoupCrawl

class AngellistCrawler(object):
    def __init__(self):
        '''
        :return:
        '''
        self.profile_crawler = AngellistProfileCrawler()

    def search_person(self,name,company):
        '''
        :param name:
        :return:
        '''
        name_url = re.sub(' ','+',name)
        url = 'https://angel.co/search?q='+name_url+'&type=people'
        soup = BeautifulsoupCrawl.single_wp(url)
        res = soup.find('div',{'class':'results-list'}).findAll('div',{'class':'result'})
        res_links = [i.find('a')['href'] for i in res]
        name_cleaned = re.sub(r'[^a-zA-Z0-9]','',name).lower()
        company_cleaned = re.sub(company_stops_regex,'',company,flags=re.IGNORECASE)
        company_cleaned = re.sub(r'[^a-zA-Z0-9]','',company_cleaned).lower()
        for link in res_links:
            dets = self.profile_crawler.fetch_details_urlinput(link)
            angel_name, angel_companies, angel_location = dets['Name'], dets['Companies'], dets['Location']
            if angel_name and angel_companies and angel_location:
                angel_name_cleaned = re.sub(r'[^a-zA-Z0-9]','',angel_name).lower()
                # check for names matching
                if name_cleaned not in angel_name_cleaned and angel_name_cleaned not in name_cleaned:
                    if Levenshtein.distance(str(name_cleaned),str(angel_name_cleaned))>10\
                            or distance.jaccard(str(name_cleaned),str(angel_name_cleaned))>=0.5:
                        continue
                # check for companies matching
                angel_companies_cleaned = re.sub(r'[^a-zA-Z0-9]','',angel_companies).lower()
                if company_cleaned in angel_companies_cleaned:
                    details_fetched = {'Name':name,'Company':company,'Location':angel_location,
                                       'AllCompanies':angel_companies,'res_from':'Angel List Match'}
                    found_person = True
                    return (details_fetched['Location'],details_fetched,found_person)
        return '',{},False


class AngellistProfileCrawler(object):
    def __init__(self):
        '''
        :return:
        '''
        pass

    def fetch_details_urlinput(self,url,outs_needed=[]):
        '''
        :param url:
        :param outs_needed:
        :return:
        '''
        if not outs_needed:
            # outs_needed = ['Name','Position','Company','CompanyLinkedinPage','Location','PreviousCompanies','Education']
            outs_needed = ['Location','Companies','Name']
        soup = BeautifulsoupCrawl.single_wp(url)
        outs = self.fetch_details_soupinput(soup,outs_needed)
        return outs

    def fetch_details_soupinput(self,soup,outs_needed=[]):
        '''
        :param soup:
        :param outs_needed:
        :return:
        '''
        if not outs_needed:
            # outs_needed = ['Name','Position','Company','CompanyLinkedinPage','Location','PreviousCompanies','Education']
            outs_needed = ['Location','Companies','Name']
        outs = {}
        if 'Location' in outs_needed:
            outs['Location'] = self.get_location(soup)
        if 'Companies' in outs_needed:
            outs['Companies'] = self.get_companies(soup)
        if 'Name' in outs_needed:
            outs['Name'] = self.get_name(soup)
        return outs

    def get_location(self,soup):
        '''
        :param soup:
        :return:
        '''
        try:
            return soup.find('div',{"class":"js-header-complete"}).find('div',{'class':'tags'}).\
                find('span',{'class':'fontello-location icon'}).next.next.text
        except:
            return ''

    def get_companies(self,soup):
        '''
        :param soup:
        :return:
        '''
        try:
            comps = list(set([i.text for i in soup.find('div',{'class':'portfolio'}).findAll('a')])-set(['']))
            return ', '.join(comps)
        except:
            return ''

    def get_name(self,soup):
        '''
        :param soup:
        :return:
        '''
        try:
            return soup.find('h1',{'itemprop':'name'}).text
        except:
            return ''