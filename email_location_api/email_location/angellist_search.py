__author__ = 'joswin'

import re
import Levenshtein
import distance
from constants import company_stops_regex

from bs_crawl import BeautifulsoupCrawl

class Utils(object):
    '''
    '''
    def __init__(self):
        pass

    def name_match(self,name,page_name):
        '''
        :param name: name
        :param page_name: name extracted from page
        :return:
        '''
        name_cleaned = re.sub(r'[^a-zA-Z0-9]','',name).lower()
        page_name_cleaned = re.sub(r'[^a-zA-Z0-9]','',page_name).lower()
        if name_cleaned not in page_name_cleaned and page_name_cleaned not in name_cleaned:
            if Levenshtein.distance(str(name_cleaned),str(page_name_cleaned))>10\
                or distance.jaccard(str(name_cleaned),str(page_name_cleaned))>=0.5:
                return False
            else:
                return True
        else:
            return True

    def company_match(self,company,page_company):
        '''
        :param company: company
        :param page_company: company extracted from page
        :return:
        '''
        company_cleaned = re.sub(company_stops_regex,'',company,flags=re.IGNORECASE)
        company_cleaned = re.sub(r'[^a-zA-Z0-9]','',company_cleaned).lower()
        page_company_cleaned = re.sub(r'[^a-zA-Z0-9]','',page_company).lower()
        if company_cleaned in page_company_cleaned:
            return True
        else:
            return False

class AngellistCrawler(object):
    def __init__(self):
        '''
        :return:
        '''
        self.profile_crawler = AngellistProfileCrawler()
        self.utils = Utils()

    def search_person(self,name,company,no_results = 5):
        '''
        :param name:
        :param no_results : no of links to look into
        :return:
        '''
        name_url = re.sub(' ','+',name)
        url = 'https://angel.co/search?q='+name_url+'&type=people'
        soup = BeautifulsoupCrawl.single_wp(url)
        res = soup.find('div',{'class':'results-list'}).findAll('div',{'class':'result'})
        res_links = [i.find('a')['href'] for i in res][:no_results]
        # if company part is not there, give the first result
        if not company:
            # if no company only select the first link
            dets = self.profile_crawler.fetch_details_urlinput(res_links[0])
            angel_name, angel_companies, angel_location = dets['Name'], dets['Companies'], dets['Location']
            if angel_name and angel_location:
                if self.utils.name_match(name,angel_name):
                    details_fetched = {'Name':name,'Location':angel_location,
                                           'AllCompanies':angel_companies,'res_from':'Angel List only Name match'}
                    found_person = True
                    return (details_fetched['Location'],details_fetched,found_person)
        else:
            # if company present, execute from here
            for link in res_links:
                dets = self.profile_crawler.fetch_details_urlinput(link)
                angel_name, angel_companies, angel_location = dets['Name'], dets['Companies'], dets['Location']
                if angel_name and angel_location:
                    # check for names matching
                    if not self.utils.name_match(name,angel_name):
                        continue
                    # check for companies matching
                    if self.utils.company_match(company,angel_companies):
                        details_fetched = {'Name':name,'Company':company,'Location':angel_location,
                                           'AllCompanies':angel_companies,'res_from':'Angel List Company+Name Match'}
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