"""
File : email_location_finder.py
Created On: 07-Mar-2016
Author: ideas2it
"""
import re

from constants import company_stops_regex,mail_end_regex
from duckduckgo_crawler import DuckduckgoCrawler
from linkedin_matcher import LinkedinLocationMatcher
from angellist_search import AngellistCrawler

from constants import confidences

class Utils(object):
    '''
    '''
    def __init__(self):
        ''' '''
        pass

    def remove_mail_end(self,mail):
        ''' Remove end part of mails like .com, .org etc'''
        regex = re.compile(mail_end_regex,re.IGNORECASE)
        try:
            ind = min([i.start() for i in regex.finditer(mail)])
            mail = mail[:ind]
            mail = re.sub(company_stops_regex,'',mail)
            return mail
        except:
            return mail
    def remove_common_mail(self,mail):
        regex = re.compile(r'gmail|yahoo|outlook')
        return regex.sub('',mail)

utils = Utils()

class EmailLocationFinder(object):
    '''
    '''
    def __init__(self):
        '''
        :return:
        '''
        self.linkedin_matcher = LinkedinLocationMatcher()
        self.angellist_crawler = AngellistCrawler()

    def ddg_linkedin_location(self,name,company,email):
        '''
        :param name:
        :param company:
        :param email:
        :return:
        '''
        if name and company:
            company = company.lower()
            company = re.sub(company_stops_regex,'',company)
            search_string = name+' '+company+' linkedin'
        elif email:
            name,company = email.split('@')
            company = utils.remove_mail_end(company)
            company = utils.remove_common_mail(company)
            search_string = name+' '+company+' linkedin'
        elif name and not company:
            search_string = name+' linkedin'
        else:
            return '',{}
        search_results = DuckduckgoCrawler().fetch_results(search_string)
        if name and company:
            location,other_details,found_person,_ = self.linkedin_matcher.linkedin_name_company_match(search_results,name,company,5)
            if found_person:
                return location,other_details
            location,other_details,found_company,fetched_details = self.linkedin_matcher.linkedin_top_company_match(search_results,company,4)
            if found_company:
                return location,other_details
            # if company not found, return the first result as company
            if fetched_details:
                url, company_details = fetched_details[0]
                if 'Headquarters' in company_details:
                    location = company_details['Headquarters']
                    other_details = {'url':url,'Headquarters':company_details['Headquarters']
                               , 'res_from':'company page, but company names not matching'}
                    return location,other_details
        elif name and not company:
            location,other_details,found_person = self.linkedin_matcher.linkedin_top_person_match(search_results,4)
            if found_person:
                return location,other_details
        return '',{}

    def get_location_ddg_linkedin_dictinput(self,args_dict):
        '''
        :param args_dict: dictionary with keys 'Name','Company' or with key 'email'
        :return:
        '''
        try:
            args_dict['Name'] = args_dict.get('Name','')
            args_dict['Company'] = args_dict.get('Company','')
            args_dict['Email'] = args_dict.get('Email','')
            location,dets = self.ddg_linkedin_location(args_dict['Name'],args_dict['Company'],args_dict['Email'])
            res_from = dets['res_from']
            if res_from in confidences:
                confidence = confidences[res_from]
                return {'Location':location,'Confidence':confidence}
            else:
                return {'Location':location,'Confidence':confidences['None']}
        except:
            return {'Location':'','Confidence':0}

    def get_location_angellist_dictinput(self,args_dict):
        '''
        :param args_dict:
        :return:
        '''
        try:
            args_dict['Name'] = args_dict.get('Name','')
            args_dict['Company'] = args_dict.get('Company','')
            args_dict['Email'] = args_dict.get('Email','')
            location,dets = self.angellist_find_loc(args_dict['Name'],args_dict['Company'],args_dict['Email'])
            res_from = dets['res_from']
            if res_from in confidences:
                confidence = confidences[res_from]
                return {'Location':location,'Confidence':confidence}
            else:
                return {'Location':location,'Confidence':confidences['None']}
        except:
            return {'Location':'','Confidence':0}

    def angellist_find_loc(self,name,company,email):
        '''
        :param name:
        :param company:
        :param email:
        :return:
        '''
        try:
            if name and company:
                location,details,found_person = self.angellist_crawler.search_person(name,company)
                return location,details
            elif email:
                name_part,company_part = email.split('@')
                company_part = utils.remove_mail_end(company_part)
                company_part = utils.remove_common_mail(company_part)
                location,details,found_person = self.angellist_crawler.search_person(name_part,company_part)
                return location,details
            elif name and not company:
                location,details,found_person = self.angellist_crawler.search_person(name,'')
                return location,details
            else:
                return '',{}
        except:
            return '',{}
