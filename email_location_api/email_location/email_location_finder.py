"""
File : email_location_finder.py
Created On: 07-Mar-2016
Author: ideas2it
"""
import re

from constants import company_stops_regex,mail_end_regex
from duckduckgo_crawler import DuckduckgoCrawler
from linkedin_matcher import LinkedinLocationMatcher

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

utils = Utils()

class EmailLocationFinder(object):
    '''
    '''
    def __init__(self):
        '''
        :return:
        '''
        self.linkedin_matcher = LinkedinLocationMatcher()

    def get_email_loc_linkedin_duckduckgo(self,email):
        '''
        :param email:
        :return:
        '''
        #split the email and search in duckduckgo
        # pdb.set_trace()
        name_part,company_part = email.split('@')
        company_part = utils.remove_mail_end(company_part)
        search_string = name_part+' '+company_part+' linkedin'
        search_results = DuckduckgoCrawler().fetch_results(search_string)
        #if gmail,yahoo etc, dont match the company part. select the top result
        if not re.search(r'gmail|yahoo|outlook',company_part):
            # location,other_details,found_person = self.linkedin_top_profile_company_match(search_results,name_part,company_part,5)
            location,other_details,found_person,_ = self.linkedin_matcher.linkedin_name_company_match(search_results,name_part,company_part,5)
            if found_person:
                return location,other_details
            # else:
            #     search_results = GoogleCrawler().fetch_results(search_string)
            #     location,other_details,found_person = self.linkedin_top_profile_company_match(search_results,name_part,company_part,5)
            #     if found_person:
            #         return location,other_details
        else:
            location,other_details,found_person = self.linkedin_matcher.linkedin_top_person_match(search_results,4)
            if found_person:
                return location,other_details
        #if no results, search only for company
        search_results = DuckduckgoCrawler().fetch_results(company_part+' linkedin')
        location,other_details,found_person,fetched_details = self.linkedin_matcher.linkedin_top_company_match(search_results,company_part,4)
        return location,other_details


    def ddg_linkedin_name_company_match(self,name,company,single_query='',flag_single=False):
        '''
        :param name:
        :param company:
        :param single_query : directly give the query here instead of code constructing the query
        :return:
        '''
        # pdb.set_trace()
        if flag_single:
            search_results = DuckduckgoCrawler().fetch_results(single_query)
        else:
            company = company.lower()
            company = re.sub(company_stops_regex,'',company)
            search_string = name+' '+company+' linkedin'
            search_results = DuckduckgoCrawler().fetch_results(search_string)
        location,other_details,found_person,_ = self.linkedin_matcher.linkedin_name_company_match(search_results,name,company,5)
        if found_person:
            return (location, other_details)
        search_results = DuckduckgoCrawler().fetch_results(company+' linkedin')
        location,other_details,found_company,fetched_details = self.linkedin_matcher.linkedin_top_company_match(search_results,company,4)
        if found_company:
            return location,other_details
        if fetched_details:
            url, company_details = fetched_details[0]
            if 'Headquarters' in company_details:
                location = company_details['Headquarters']
                other_details = {'url':url,'Headquarters':company_details['Headquarters']
                           , 'res_from':'company page'}
            else:
                other_details = {'url':url,'Headquarters':'No page','res_from':'company page'}
            return location,other_details
        return '',{}

    def get_location_ddg_linkedin(self,args_dict):
        '''
        :param args_dict: dictionary with keys 'Name','Company' or with key 'email'
        :return:
        '''
        try:
            if 'Name' in args_dict and 'Company' in args_dict:
                location,dets = self.ddg_linkedin_name_company_match(args_dict['Name'],args_dict['Company'])
            elif 'Email' in args_dict:
                location,dets = self.get_email_loc_linkedin_duckduckgo(args_dict['Email'])
            else:
                return {'Location':'','Confidence':0}
            res_from = dets['res_from']
            if res_from in confidences:
                confidence = confidences[res_from]
            return {'Location':location,'Confidence':confidence}
        except:
            return {'Location':'','Confidence':0}