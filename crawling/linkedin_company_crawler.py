"""
File : linkedin_organization_service.py
Created On: 23-Sep-2015
Author: ideas2it
"""
import re
from bs_crawl import BeautifulsoupCrawl
import linkedin_parser
import logging

all_org_cases = ['Specialties','Website','Industry','Type','Headquarters','Company Size','Founded',\
                     'Company Name','Description Text','Employee Details','Also Viewed Companies']


def dec_fun(fn):
    '''
    if fn executions into error, None is returned
    :param fn:
    :return:
    '''
    def new_fun(*args,**kwargs):
        try:
            return fn(*args,**kwargs)
        except Exception as e:
            logging.error('Exception while fetching details from company page :', exc_info=True)
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
        except Exception as e:
            logging.error('Exception while running main from company page :', exc_info=True)
            return None
    return new_fun1



class LinkedinOrganizationService(object):
    def __init__(self):
        # print('class intializing')
        self._crawler = BeautifulsoupCrawl.single_wp
        self.link_parser = linkedin_parser.LinkedinParserSelenium('','')


    @complete_cases_org
    def get_organization_details_from_linkedin_link(self,url,use_selenium=True):
        '''
        :param url: linkedin company url
        :return:
        '''
        if use_selenium:
            self.soup = self.link_parser.get_soup(url)
        else:
            self.soup = self._crawler(url)
        self.details = {}
        self.get_name()
        self.get_description()
        self.get_details()
        self.get_employees()
        self.get_also_viewed()
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

    @dec_fun
    def get_employees(self):
        '''
        :return:
        '''
        p_list = self.soup.find('div',{'class':'company-employees module'}).findAll('li')
        out_list = []
        for tmp in p_list:
            tmp_dic = {}
            try:
                tmp_dic['linkedin_url'] = tmp.find('a')['href']
            except:
                tmp_dic['linkedin_url'] = ''
            try:
                tmp_dic['Name'] = tmp.find('dt').text
            except:
                tmp_dic['Name'] = ''
            try:
                tmp_dic['Designation'] = tmp.find('dd').text
            except:
                tmp_dic['Designation'] = ''
            out_list.append(tmp_dic)
        self.details['Employee Details'] = out_list

    @dec_fun
    def get_also_viewed(self):
        '''
        :return:
        '''
        p_list = self.soup.find('div',{'class':'also-viewed module'}).findAll('li')
        out_list = []
        for tmp in p_list:
            tmp_dic = {}
            try:
                tmp_dic['company_linkedin_url'] = tmp.find('a')['href']
            except:
                tmp_dic['company_linkedin_url'] = ''
            try:
                tmp_dic['Company Name'] = tmp.find('a').find('img')['alt']
            except:
                tmp_dic['Company Name'] = ''
            out_list.append(tmp_dic)
        self.details['Also Viewed Companies'] = out_list