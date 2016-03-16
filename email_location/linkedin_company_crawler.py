"""
File : linkedin_organization_service.py
Created On: 23-Sep-2015
Author: ideas2it
"""
import re
from bs_crawl import BeautifulsoupCrawl

all_org_cases = ['Specialties','Website','Industry','Type','Headquarters','Company Size','Founded',\
                     'Company Name','Description Text']

def dec_fun(fn):
    '''
    if fn executions into error, None is returned
    :param fn:
    :return:
    '''
    def new_fun(*args,**kwargs):
        try:
            return fn(*args,**kwargs)
        except:
            return None
    return new_fun

def complete_cases_org(fn):
    '''
    :param fn:
    :return:
    '''
    def new_fun1(*args,**kwargs):
        try:
            out = fn(*args,**kwargs)
            out1 = {}
            for i in all_org_cases:
                try:
                    out1[i] = out[i]
                except:
                    out1[i] = ''
            return out1
        except:
            return None
    return new_fun1



class LinkedinOrganizationService:
    def __init__(self):
        # print('class intializing')
        self._crawler = BeautifulsoupCrawl.single_wp

    @complete_cases_org
    def get_organization_details_from_linkedin_link(self,url):
        '''
        :param url: linkedin company url
        :return:
        '''
        self.soup = self._crawler(url)
        self.details = {}
        self.get_name()
        self.get_description()
        self.get_details()
        return self.details

    @dec_fun
    def get_description(self):
        '''
        :return:
        '''
        para = self.soup.find("div", {"class": "basic-info-description"}).findChild().getText()
        para = re.sub(r'[.]+','. ',re.sub(r'[\r\n]','.',para))
        self.details['Description Text'] = para

    @dec_fun
    def get_name(self):
        '''
        :return:
        '''
        self.details['Company Name'] = self.soup.find("div",{"class":"content-wrapper"}).find("h1").findChild().getText().strip()

    @dec_fun
    def get_details(self):
        '''
        :return:
        '''
        headers = ['h3','h4']
        for header in headers:
            for tag in self.soup.find("div",{"class":"basic-info-about"}).findAll(header):
                self.details[tag.getText().strip()] = tag.find_next().getText().strip()