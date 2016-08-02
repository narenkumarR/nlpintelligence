"""
File : linkedin_profile_crawler.py
Created On: 07-Mar-2016
Author: ideas2it
"""
from bs_crawl import BeautifulsoupCrawl
# from bs4 import BeautifulSoup

class LinkedinProfileCrawler(object):
    '''Crawl a linkedin profie page
    '''
    def __init__(self):
        '''
        :return:
        '''
        pass

    def fetch_details_urlinput(self,url,outs_needed=[]):
        '''
        :param url:
        :param outs_needed: list of parameters need to be fetched. if empty, all are fetched
        :return: dictionary with the fetched details
        '''
        soup = BeautifulsoupCrawl.single_wp(url)
        outs = self.fetch_details_soupinput(soup,outs_needed)
        return outs

    def fetch_details_soupinput(self,soup,outs_needed=[]):
        '''
        :param soup:
        :param outs_needed: list of parameters need to be fetched. if empty, all are fetched
        :return: dictionary with the fetched details
        '''
        if not outs_needed:
<<<<<<< HEAD
            outs_needed = ['Name','Position','Company','CompanyLinkedinPage','Location','PreviousCompanies','Education']
=======
            outs_needed = ['Name','Position','Company','CompanyLinkedinPage','Location','PreviousCompanies']
>>>>>>> first commit
        outs = {}
        if 'Name' in outs_needed:
            outs['Name'] = self.get_name(soup)
        if 'Position' in outs_needed:
            outs['Position'] = self.get_position(soup)
        if 'Company' in outs_needed:
            outs['Company'] = self.get_company(soup)
        if 'Location' in outs_needed:
            outs['Location'] = self.get_location(soup)
        if 'CompanyLinkedinPage' in outs_needed:
            outs['CompanyLinkedinPage'] = self.get_company_linkedin_page(soup)
        if 'PreviousCompanies' in outs_needed:
            outs['PreviousCompanies'] = self.get_previous_companies(soup)
<<<<<<< HEAD
        if 'Education' in outs_needed:
            outs['Education'] = self.get_education(soup)
=======
>>>>>>> first commit
        return outs

    def get_name(self,soup):
        '''
        :param soup:
        :return:
        '''
        try:
            return soup.find('div',{'id':'profile'}).find('div',{'class':'profile-overview-content'}).find('h1',{'id':'name'}).text
        except:
            return ''

    def get_position(self,soup):
        '''
        :param soup:
        :return:
        '''
        try:
            return soup.find('div',{'id':'profile'}).find('div',{'class':'profile-overview-content'})\
                .find('p',{'class':'headline title'}).text
        except:
            return ''

    def get_company(self,soup):
        '''
        :param soup:
        :return:
        '''
        try:
            return soup.find('div',{'id':'profile'}).find('div',{'class':'profile-overview-content'})\
                .find('table',{'class':'extra-info'}).find('td').text
        except:
            return ''

    def get_company_linkedin_page(self,soup):
        '''
        :param soup:
        :return:
        '''
        try:
<<<<<<< HEAD
            tmp = soup.find('div',{'id':'profile'}).find('div',{'class':'profile-overview-content'}).\
                find('table',{'class':'extra-info'}).find('tr').findAll('a')
            links = [i['href'] for i in tmp]
            return ','.join(links)
=======
            return soup.find('div',{'id':'profile'}).find('div',{'class':'profile-overview-content'})\
                .find('table',{'class':'extra-info'}).find('span',{'class':'org'}).find('a')['href']
>>>>>>> first commit
        except:
            return ''

    def get_location(self,soup):
        '''
        :param soup:
        :return:
        '''
        try:
            return soup.find('div',{'id':'profile'}).find('div',{'class':'profile-overview-content'})\
                .find('span',{'class':'locality'}).text
        except:
            return ''

    def get_previous_companies(self,soup):
        '''
        :param soup:
        :return:
        '''
        try:
            return soup.find('div',{'id':'profile'}).find('div',{'class':'profile-overview-content'})\
<<<<<<< HEAD
                .find('table',{'class':'extra-info'}).find('tr',{'data-section':'pastPositionsDetails'}).find('td').text
        except:
            return ''

    def get_education(self,soup):
        '''
        :param soup:
        :return:
        '''
        try:
            return soup.find('div',{'id':'profile'}).find('div',{'class':'profile-overview-content'}).\
                find('table',{'class':'extra-info'}).find('tr',{'data-section':'educationsDetails'}).find('td').text
=======
                .find('table',{'class':'extra-info'}).find('tr',{'data-section':'pastPositions'}).find('td').text
>>>>>>> first commit
        except:
            return ''