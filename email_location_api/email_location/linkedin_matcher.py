__author__ = 'joswin'

import Levenshtein
import distance
import re

from linkedin_profile_crawler import LinkedinProfileCrawler
from linkedin_company_crawler import LinkedinOrganizationService
from bs_crawl import BeautifulsoupCrawl
from constants import company_stops_regex

class LinkedinLocationMatcher(object):
    ''' class to match location and name company details.
    '''
    def __init__(self):
        '''
        :return:
        '''
        pass

    def linkedin_top_company_match(self,search_results,company_part,limit_results=None):
        '''
        From the search results of linkedin pages of companies, match the company names and give the company location
        :param search_results: List of dictionaries. Each dictionary must have 'url' key, with the linkedin url
        :param company_part: company name
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
                        , 'res_from':'company page, company names matching'}
                    found_company = True
                    return (location,other_details,found_company,fetched_details)
        return location,other_details,found_company,fetched_details

    def linkedin_top_person_match(self,search_results,limit_results = None):
        '''
        From search results, get the top profile page, and give its location as output
        :param search_results:
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
                other_details = {'url':res['url'], 'company':linkedin_details['Company']
                                    ,'position':linkedin_details['Position']
                                    ,'prev companies':linkedin_details['PreviousCompanies']
                                    , 'res_from':'profile page without company match'}
                found_person = True
                return (location,other_details,found_person)
        return (location,other_details,found_person)

    def linkedin_name_company_match(self,search_results,name_part,company_part,limit_results = None):
        '''
        For profiles in the search result, output the profile which has a matching company name
        :param search_results: List of dictionaries. Each dictionary must have 'url' key, with the linkedin url
        :param company_part:
        :param limit_results: (number) limit the no of results to look into. By default, look into all the results
        :return:
        '''
        linkedin_results = [res for res in search_results if 'linkedin' in res['url']]
                                     # and not re.search(r'/pub/dir',res['url'])] #removed because sometimes the results
                                                                            #are given in this format
        if limit_results:
            linkedin_results = linkedin_results[:limit_results]
        details_fetched, found_person = {}, False
        comp_part_cleaned = re.sub(r'[^a-zA-Z0-9]','',company_part).lower()
        name_part_cleaned = re.sub(r'[^a-zA-Z0-9]','',name_part).lower()
        linkedin_extracted = [] # the best result till now
        additional_depth, additional_depth_stop = 0,1
        for res in linkedin_results:
            if additional_depth >= additional_depth_stop:
                break
            if found_person:
                additional_depth += 1
            try:
                url = res['url']
                soup = BeautifulsoupCrawl.single_wp(url)
                linkedin_details = LinkedinProfileCrawler().fetch_details_soupinput(soup)
                if linkedin_details['Location']:
                    linkedin_extracted.append((url,linkedin_details))
                    position_linkedin = linkedin_details['Position']
                    company_linkedin = linkedin_details['Company']
                    prevcompanies_linkedin = linkedin_details['PreviousCompanies']
                    # clean the linkedin name for matching
                    linkedin_name_cleaned = re.sub(r'[^a-zA-Z0-9]','',linkedin_details['Name']).lower()
                    # if names not similar, continue
                    if name_part_cleaned not in linkedin_name_cleaned and linkedin_name_cleaned not in name_part_cleaned:
                        if Levenshtein.distance(str(name_part_cleaned),str(linkedin_name_cleaned))>10\
                                or distance.jaccard(str(name_part_cleaned),str(linkedin_name_cleaned))>=0.5\
                                or len(set.intersection(set(linkedin_details['Name'].split()),set(name_part.split())))==0:
                            continue
                    # clean company & position for matching
                    position_cleaned = re.sub(r'[^a-zA-Z0-9]','',position_linkedin).lower()
                    company_cleaned = re.sub(r'[^a-zA-Z0-9]','',company_linkedin).lower()
                    prev_company_cleaned = re.sub(r'[^a-zA-Z0-9]','',prevcompanies_linkedin).lower()
                    # create regex for for matching
                    prevcompanies_list = prevcompanies_linkedin.lower().split(', ')
                    prevcompanies_list = [re.sub(company_stops_regex,'',prev_comp) for prev_comp in prevcompanies_list]
                    prevcompanies_list = [re.sub(r'[^a-zA-Z0-9]','',prev_comp) for prev_comp in prevcompanies_list]
                    prevcompanies_list = [prev_comp for prev_comp in prevcompanies_list if prev_comp]
                    prev_company_regex = '|'.join(prevcompanies_list)
                    # random text if no regex created
                    if not prev_company_regex:
                        prev_company_regex = 'randomfljsdafhdsah'
                    company_list = company_linkedin.lower().split(', ')
                    company_list = [re.sub(company_stops_regex,'',comp) for comp in company_list if comp]
                    company_list = [re.sub(r'[^a-zA-Z0-9]','',comp) for comp in company_list]
                    clean_list = company_list + [position_cleaned]
                    clean_list = [i for i in clean_list if i]
                    if clean_list:
                        regex_phrase = '|'.join(clean_list)
                        if (re.search(comp_part_cleaned,position_cleaned) or re.search(comp_part_cleaned,company_cleaned)
                                or re.search(regex_phrase,comp_part_cleaned)) :
                            details_fetched = {'url':url, 'company':company_linkedin,'position':position_linkedin
                                         , 'prev companies':prevcompanies_linkedin,'Location':linkedin_details['Location']
                                        , 'res_from':'profile page with company match'}
                            found_person = True
                            return (details_fetched['Location'],details_fetched,found_person,linkedin_extracted)

                    # Here found_person set to True only when there was a match from the text part. In that case
                    # we will go to next links (till additional_depth_stop), and check for match only in current company.
                    # If no match, go with the first result itself
                    if not found_person:
                        if (re.search(comp_part_cleaned,prev_company_cleaned)
                                        or re.search(prev_company_regex,comp_part_cleaned)):
                            details_fetched = {'url':url, 'company':company_linkedin,'position':position_linkedin
                                                    ,'prev companies':prevcompanies_linkedin,'Location':linkedin_details['Location']
                                                    ,'res_from':'profile page with previous company match'}
                            found_person = True
                            continue
                        #search for the company name in the entire text(profile part of the page)
                        if re.search(comp_part_cleaned,re.sub(r'[^a-zA-Z0-9]','',soup.find('div',{'id':'profile'}).text),re.IGNORECASE):
                            details_fetched = {'url':url, 'company':company_linkedin,'position':position_linkedin
                                             , 'prev companies':prevcompanies_linkedin,'Location':linkedin_details['Location']
                                , 'res_from':'profile page with company match on page text'}
                            found_person = True
                            continue
                        #search for the company name in the entire text(rest of the page(people who viewed this also viewed))
                        if re.search(comp_part_cleaned,re.sub(r'[^a-zA-Z0-9]','',soup.find('div',{'id':'aux'}).text),re.IGNORECASE):
                            details_fetched = {'url':url, 'company':company_linkedin,'position':position_linkedin
                                             , 'prev companies':prevcompanies_linkedin,'Location':linkedin_details['Location']
                                , 'res_from':'profile page with company match on page text (also viewed part)'}
                            found_person = True
                            continue
            except:
                continue
        if 'Location' in details_fetched:
            location = details_fetched['Location']
        else:
            location = ''
        return (location,details_fetched,found_person,linkedin_extracted)

