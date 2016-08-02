"""
File : email_location_finder.py
Created On: 07-Mar-2016
Author: ideas2it
"""
<<<<<<< HEAD
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
=======
import Levenshtein
import distance
import re
# import nltk
import pdb

from duckduckgo_crawler import DuckduckgoCrawler
from google_crawler import GoogleCrawler
from linkedin_profile_crawler import LinkedinProfileCrawler
from linkedin_company_crawler import LinkedinOrganizationService
from bs_crawl import BeautifulsoupCrawl

company_stops_regex = 'inc|limited|ltd|technologies|tecnology|services|service|llc'
>>>>>>> first commit

class EmailLocationFinder(object):
    '''
    '''
    def __init__(self):
        '''
        :return:
        '''
<<<<<<< HEAD
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
=======
        pass

    def get_email_loc_linkedin_duckduckgo(self,email):
        '''
        :param email:
        :return:
        '''
        #split the email and search in duckduckgo
        # pdb.set_trace()
        name_part,company_part = email.split('@')
        search_string = name_part+' '+company_part+' linkedin'
        search_results = DuckduckgoCrawler().fetch_results(search_string)
        #if gmail,yahoo etc, dont match the company part. select the top result
        if not re.search(r'gmail|yahoo|outlook',company_part):
            location,other_details,found_person = self.linkedin_top_profile_company_match(search_results,name_part,company_part,5)
            if found_person:
                return location,other_details
            # else:
            #     search_results = GoogleCrawler().fetch_results(search_string)
            #     location,other_details,found_person = self.linkedin_top_profile_company_match(search_results,name_part,company_part,5)
            #     if found_person:
            #         return location,other_details
        else:
            location,other_details,found_person = self.linkedin_top_person_match(search_results,4)
            if found_person:
                return location,other_details
        # now try without matching company names
        # location,other_details,found_person = self.linkedin_top_person_match(search_results,4)
        # if found_person:
        #     return location,other_details
        #try with google match

        # Remove the .com,.co.in etc and search again. not needed as duckduckgo automatically gives it in earlier search
        # dot_match = re.search('\.',company_part).start()
        # if dot_match:
        #     company_part = company_part[:dot_match]
        # search_string = name_part+' '+company_part+' '+'linkedin'
        # search_results = DuckduckgoCrawler().fetch_results(search_string)
        # linkedin_results = [res for res in search_results if 'linkedin' in res['url']]
        # location,other_details,found_person = self.linkedin_top_profile_company_match(linkedin_results,company_part)
        # if not found_person:
        # searching through companies if no results obtained
        search_results = DuckduckgoCrawler().fetch_results(company_part+' linkedin')
        location,other_details = self.linkedin_top_company_match(search_results,company_part,4)
        return location,other_details

    def linkedin_top_profile_company_match(self,search_results,name_part,company_part,limit_results = None):
        '''
        For profiles in the search result, output the profile which has a matching company name
        :param linkedin_results: List of dictionaries. Each dictionary must have 'url' key, with the linkedin url
        :param company_part:
        :param limit_results: (number) limit the no of results to look into. By default, look into all the results
        :return:
        '''
        linkedin_results = [res for res in search_results if 'linkedin' in res['url']]
        if limit_results:
            linkedin_results = linkedin_results[:limit_results]
        location, other_details, found_person = '', {}, False
        dot_match = re.search('\.',company_part).start()
        if dot_match:
            company_part = company_part[:dot_match]
        comp_part_cleaned = re.sub(r'[^a-zA-Z0-9]','',company_part).lower()
        name_part_cleaned = re.sub(r'[^a-zA-Z0-9]','',name_part).lower()
        for res in linkedin_results:
            url = res['url']
            soup = BeautifulsoupCrawl.single_wp(url)
            linkedin_details = LinkedinProfileCrawler().fetch_details_soupinput(soup)
            location = linkedin_details['Location']
            if location:
                position_linkedin = linkedin_details['Position']
                company_linkedin = linkedin_details['Company']
                prevcompanies_linkedin = linkedin_details['PreviousCompanies']
                linkedin_name_cleaned = re.sub(r'[^a-zA-Z0-9]','',linkedin_details['Name']).lower()
                if Levenshtein.distance(str(name_part_cleaned),str(linkedin_name_cleaned))>10 \
                        or distance.jaccard(str(name_part_cleaned),str(linkedin_name_cleaned))>=0.5:
                    continue
                position_cleaned = re.sub(r'[^a-zA-Z0-9]','',position_linkedin).lower()
                company_cleaned = re.sub(r'[^a-zA-Z0-9]','',company_linkedin).lower()
                prev_company_cleaned = re.sub(r'[^a-zA-Z0-9]','',prevcompanies_linkedin).lower()
                clean_list = [position_cleaned,company_cleaned]
                clean_list = [i for i in clean_list if i]
                if clean_list:
                    regex_phrase = '|'.join(clean_list)
                    if (re.search(comp_part_cleaned,position_cleaned) or re.search(comp_part_cleaned,company_cleaned)\
                            or re.search(regex_phrase,comp_part_cleaned) or re.search(comp_part_cleaned,prev_company_cleaned)):
                        other_details = {'url':res['url'], 'company':company_linkedin,'position':position_linkedin\
                                         , 'prev companies':prevcompanies_linkedin\
                            , 'res_from':'profile page with company match'}
                        found_person = True
                        return (location,other_details,found_person)
                #search for the company name in the entire text
                if re.search(comp_part_cleaned,soup.text,re.IGNORECASE):
                    location = linkedin_details['Location']
                    other_details = {'url':res['url'], 'company':company_linkedin,'position':position_linkedin\
                                     , 'prev companies':prevcompanies_linkedin\
                        , 'res_from':'profile page with company match on page text'}
                    found_person = True
                    return (location,other_details,found_person)
        return (location,other_details,found_person)

    def linkedin_top_company_match(self,search_results,company_part,limit_results = None):
        '''
        From the search results of linkedin pages of companies, match the company names and give the company location
        :param linedin_results: List of dictionaries. Each dictionary must have 'url' key, with the linkedin url
        :return:
        '''
        linkedin_results = [res for res in search_results if 'linkedin' in res['url']]
        if limit_results:
            linkedin_results = linkedin_results[:limit_results]
        location, other_details, found_company = '', {},False
        comp_part_cleaned = re.sub(r'[^a-zA-Z0-9]','',company_part).lower()
        fetched_details = []
        for res in linkedin_results:
            company_details = LinkedinOrganizationService().get_organization_details_from_linkedin_link(res['url'])
            company_name_cleaned = re.sub(r'[^a-zA-Z0-9]','',company_details['Company Name']).lower()
            fetched_details.append((res['url'],company_details))
            if company_details['Headquarters']:
                if (comp_part_cleaned in company_name_cleaned or company_name_cleaned in comp_part_cleaned):
                    location = company_details['Headquarters']
                    other_details = {'url':res['url'],'Headquarters':company_details['Headquarters']
                        , 'res_from':'company page'}
                    found_company = True
                    return (location,other_details,found_company,fetched_details)
        return location,other_details,found_company,fetched_details

    def linkedin_top_person_match(self,search_results,limit_results = None):
        '''
        From search results, get the top profile page, and give its location as output
        :param linkedin_results:
        :return:
        '''
        linkedin_results = [res for res in search_results if 'linkedin' in res['url']]
        if limit_results:
            linkedin_results = linkedin_results[:limit_results]
        location, other_details, found_person = '', {}, False
        for res in linkedin_results:
            linkedin_details = LinkedinProfileCrawler().fetch_details_urlinput(res['url'])
            if linkedin_details['Location']:
                location = linkedin_details['Location']
                other_details = {'url':res['url'], 'company':linkedin_details['Company'],'position':linkedin_details['Position']\
                                 ,'prev companies':linkedin_details['PreviousCompanies']
                    , 'res_from':'profile page without company match'}
                found_person = True
                return (location,other_details,found_person)
        return (location,other_details,found_person)

    def ddg_linkedin_name_company_match(self,name,company,single_query='',flag_single=False):
        '''
        :param name:
        :param company:
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
        location,other_details,found_person,_ = self.linkedin_name_company_match(search_results,name,company,5)
        if found_person:
            return (location, other_details)
        search_results = DuckduckgoCrawler().fetch_results(company+' linkedin')
        location,other_details,found_company,fetched_details = self.linkedin_top_company_match(search_results,company,4)
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

    def linkedin_name_company_match(self,search_results,name_part,company_part,limit_results = None):
        '''
        For profiles in the search result, output the profile which has a matching company name
        :param search_results: List of dictionaries. Each dictionary must have 'url' key, with the linkedin url
        :param company_part:
        :param limit_results: (number) limit the no of results to look into. By default, look into all the results
        :return:
        '''
        linkedin_results = [res for res in search_results if 'linkedin' in res['url']]
        if limit_results:
            linkedin_results = linkedin_results[:limit_results]
        location, other_details, found_person = '', {}, False
        comp_part_cleaned = re.sub(r'[^a-zA-Z0-9]','',company_part).lower()
        name_part_cleaned = re.sub(r'[^a-zA-Z0-9]','',name_part).lower()
        linkedin_extracted = []
        for res in linkedin_results:
            url = res['url']
            soup = BeautifulsoupCrawl.single_wp(url)
            linkedin_details = LinkedinProfileCrawler().fetch_details_soupinput(soup)
            location = linkedin_details['Location']
            if location:
                linkedin_extracted.append((url,linkedin_details))
                position_linkedin = linkedin_details['Position']
                company_linkedin = linkedin_details['Company']
                prevcompanies_linkedin = linkedin_details['PreviousCompanies']
                linkedin_name_cleaned = re.sub(r'[^a-zA-Z0-9]','',linkedin_details['Name']).lower()
                if Levenshtein.distance(str(name_part_cleaned),str(linkedin_name_cleaned))>10:
                    continue
                position_cleaned = re.sub(r'[^a-zA-Z0-9]','',position_linkedin).lower()
                company_cleaned = re.sub(r'[^a-zA-Z0-9]','',company_linkedin).lower()
                prev_company_cleaned = re.sub(r'[^a-zA-Z0-9]','',prevcompanies_linkedin).lower()
                prevcompanies_list = prevcompanies_linkedin.lower().split(', ')
                prevcompanies_list = [re.sub(company_stops_regex,'',prev_comp) for prev_comp in prevcompanies_list]
                prevcompanies_list = [re.sub(r'[^a-zA-Z0-9]','',prev_comp) for prev_comp in prevcompanies_list]
                prevcompanies_list = [prev_comp for prev_comp in prevcompanies_list if prev_comp]
                prev_company_regex = '|'.join(prevcompanies_list)
                if not prev_company_regex:
                    prev_company_regex = 'randomfljsdafhdsah'
                company_list = company_linkedin.lower().split(', ')
                company_list = [re.sub(company_stops_regex,'',comp) for comp in company_list if comp]
                company_list = [re.sub(r'[^a-zA-Z0-9]','',comp) for comp in company_list]
                clean_list = company_list + [position_cleaned]
                clean_list = [i for i in clean_list if i]
                if clean_list:
                    regex_phrase = '|'.join(clean_list)
                    if (re.search(comp_part_cleaned,position_cleaned) or re.search(comp_part_cleaned,company_cleaned)\
                            or re.search(regex_phrase,comp_part_cleaned) or re.search(comp_part_cleaned,prev_company_cleaned)\
                            or re.search(prev_company_regex,comp_part_cleaned)):
                        other_details = {'url':url, 'company':company_linkedin,'position':position_linkedin\
                                         , 'prev companies':prevcompanies_linkedin\
                            , 'res_from':'profile page with company match'}
                        found_person = True
                        return (location,other_details,found_person,linkedin_extracted)
                #search for the company name in the entire text(profile part of the page)
                if re.search(comp_part_cleaned,re.sub(r'[^a-zA-Z0-9]','',soup.find('div',{'id':'profile'}).text),re.IGNORECASE):
                    location = linkedin_details['Location']
                    other_details = {'url':url, 'company':company_linkedin,'position':position_linkedin\
                                     , 'prev companies':prevcompanies_linkedin\
                        , 'res_from':'profile page with company match on page text'}
                    found_person = True
                    return (location,other_details,found_person,linkedin_extracted)
                #search for the company name in the entire text(rest of the page(people who viewed this also viewed))
                if re.search(comp_part_cleaned,re.sub(r'[^a-zA-Z0-9]','',soup.find('div',{'id':'aux'}).text),re.IGNORECASE):
                    location = linkedin_details['Location']
                    other_details = {'url':url, 'company':company_linkedin,'position':position_linkedin\
                                     , 'prev companies':prevcompanies_linkedin\
                        , 'res_from':'profile page with company match on page text (also viewed part)'}
                    found_person = True
                    return (location,other_details,found_person,linkedin_extracted)
        #if not able to find, return the first profile result
        # for url,linkedin_details in linkedin_extracted:
        #     location = linkedin_details['Location']
        #     position_linkedin = linkedin_details['Position']
        #     company_linkedin = linkedin_details['Company']
        #     prevcompanies_linkedin = linkedin_details['PreviousCompanies']
        #     other_details = {'url':url, 'company':company_linkedin,'position':position_linkedin\
        #                                  , 'prev companies':prevcompanies_linkedin\
        #                     , 'res_from':'first profile result from DDG'}
        #     found_person = True
        #     return (location,other_details,found_person,linkedin_extracted)
        return (location,other_details,found_person,linkedin_extracted)

>>>>>>> first commit
