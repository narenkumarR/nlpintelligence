__author__ = 'joswin'
# -*- coding: utf-8 -*-

import logging
import json
import requests
import time
import pandas as pd
from StringIO import StringIO

from constants import accessToken

def chunker(seq, size):
    return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))

company_search_url = 'https://api.insideview.com/api/v1/companies'
contact_search_url = 'https://api.insideview.com/api/v1/target/contacts'
contact_details_url = 'https://api.insideview.com/api/v1/target/contact/{newcontactId}'
company_details_url = 'https://api.insideview.com/api/v1/company/{companyId}'
company_details_job_url = 'https://api.insideview.com/api/v1/company/job'
company_details_job_status_url = 'https://api.insideview.com/api/v1/company/job/{jobId}'
company_details_job_results_url = 'https://api.insideview.com/api/v1/company/job/{jobId}/results'
company_tech_profile_url = 'https://api.insideview.com/api/v1/company/{companyId}/techProfile'
people_search_url = 'https://api.insideview.com/api/v1/contacts'
contact_fetch_url = 'https://api.insideview.com/api/v1/contact/{contactId}'
contact_fetch_job_url = 'https://api.insideview.com/api/v1/contact/job'
contact_fetch_job_status_url = 'https://api.insideview.com/api/v1/contact/job//{jobId}'
contact_fetch_job_results_url = 'https://api.insideview.com/api/v1/contact/job//{jobId}/results'
company_name_search_insideview_url = 'https://api.insideview.com/api/v1/target/company/lookup'
company_details_search_insideview_url = 'https://api.insideview.com/api/v1/target/companies'

company_name_field,website_field = 'company_name','website'


class InsideviewDataFetcher(object):
    '''
    '''
    def __init__(self,api_counter,throttler_app_address = 'http://192.168.3.56:5000/'):
        self.api_counter = api_counter
        self.throttler_app_address = throttler_app_address

    def search_company_names_in_insideview(self,search_dic):
        ''' search insideview for company names (no other details, no website) in insideview
        page no should be given in the param_dic
        :param param_dic:
        :return:
        '''
        param_dic = {'url':company_name_search_insideview_url}
        r = requests.post(self.throttler_app_address,params=param_dic,json=search_dic)
        res_dic = json.loads(r.text)
        if not res_dic.get('companies',None):
            if res_dic.get('message') in ['request throttled by insideview','1000 per 5 minute']:
                logging.info('throttling limit reached. try this search_company_names_in_insideview later')
                return ['throttling limit reached']
            elif res_dic.get('message'):
                raise ValueError('Error happened. {}'.format(res_dic))
        if res_dic.get('companies'):
            return res_dic['companies']
        else:
            return []

    def search_company_details_in_insideview(self,search_dic):
        ''' earch insideview for company details (no name/website) in insideview
        page no should be given in the param_dic
        :param search_dic:
        :return:
        '''
        param_dic = {'url':company_details_search_insideview_url}
        r = requests.post(self.throttler_app_address,params=param_dic,json=search_dic)
        res_dic = json.loads(r.text)
        if not res_dic.get('companies',None):
            if res_dic.get('message') in ['request throttled by insideview','1000 per 5 minute']:
                logging.info('throttling limit reached. try this search_company_details_in_insideview later')
                return ['throttling limit reached']
            elif res_dic.get('message'):
                raise ValueError('Error happened. {}'.format(res_dic))
        if res_dic.get('companies'):
            return res_dic['companies']
        else:
            return []

    def search_company_single(self,name,website,country=None,state=None,city=None,max_no_results=99,results_per_page=50):
        '''
        :param name: company name for search
        :param website: website for search
        :param ticker: ticker
        :param max_no_results: save this many matching results for a company
        :param results_per_page:
        :return:
        '''
        out_list = []
        search_dic = {}
        if name:
            search_dic['name'] = name
        if website:
            search_dic['website'] = website
        if country:
            search_dic['country'] = country
        if state:
            search_dic['state'] = state
        if city:
            search_dic['city'] = city
        if not search_dic:
            raise ValueError('No searchable info about the company is present')
        search_dic['url'] = company_search_url
        search_dic['resultsPerPage'] = results_per_page
        # go through results till all search results are obtained. give maximum as 500 for safety
        total_results,res_page_no = 9999999,0
        while min(max_no_results,total_results) > res_page_no*results_per_page:
            res_page_no += 1
            search_dic['page'] = res_page_no
            r = requests.get(self.throttler_app_address,params=search_dic)
            self.api_counter.company_search_hits += 1
            res_dic = json.loads(r.text)
            if not res_dic.get('companies',None):
                if res_dic.get('message') in ['request throttled by insideview','1000 per 5 minute']:
                    logging.info('throttling limit reached. try this company later')
                    self.api_counter.company_search_hits -= 1
                    return ['throttling limit reached']
                elif res_dic.get('message'):
                    raise ValueError('Error happened. {}'.format(res_dic))
                break
            out_list.extend(res_dic.get('companies'))
            total_results = res_dic['totalResults']
        return out_list

    def get_company_details_from_id(self,company_id):
        '''
        :param company_id:
        :return:
        '''
        contact_url = company_details_url.format(companyId=company_id)
        search_dic = {'url':contact_url}
        r = requests.get(self.throttler_app_address,params=search_dic)
        res_dic = json.loads(r.text)
        return res_dic

    def get_contact_details_from_contactid(self,contact_id):
        '''
        :param contact_id:
        :return:
        '''
        contact_url = contact_fetch_url.format(contactId=contact_id)
        search_dic = {'url':contact_url}
        r = requests.get(self.throttler_app_address,params=search_dic)
        res_dic = json.loads(r.text)
        return res_dic

    def post_job_and_get_jobid(self,url,headers,body):
        '''
        :param url:
        :param headers:
        :param body:
        :return:
        '''
        while True:
            r = requests.post(url,headers=headers,data=body)
            r_dic = json.loads(r.text)
            if r_dic.get('status') == 'accepted':
                return r_dic
            time.sleep(60)

    def wait_till_job_completes(self,url,headers):
        while True:
            time.sleep(60)
            r = requests.get(url,headers=headers)
            r_dic = json.loads(r.text)
            if r_dic.get('status') == 'finished':
                return r_dic

    def get_company_details_from_ids_job(self,company_ids_list):
        '''
        :param company_ids_list:
        :return:
        '''
        #todo: only one job run at a time. maximum two possible at a time
        headers = {'accessToken':accessToken,'Accept':'application/json','Content-Type':'text/plain'}
        for comp_ids in chunker(company_ids_list,9999):
            body = ','.join([str(i) for i in comp_ids])
            r_dic = self.post_job_and_get_jobid(url=company_details_job_url,headers=headers,body=body)
            job_id = r_dic.get('jobId')
            if not job_id:
                raise ValueError('Some error happened, could not get job id from insideview')
            logging.info('waiting for company details job_id:{},no of companies to get details:{}'.format(job_id,len(comp_ids)))
            _ = self.wait_till_job_completes(url=company_details_job_status_url.format(jobId=job_id),headers=headers)
            # getting results
            r = requests.get(company_details_job_results_url.format(jobId=job_id),headers=headers)
            io_read = StringIO(r.text)
            df = pd.read_csv(io_read)
            logging.info('got results for job_id:{},no of records:{}'.format(job_id,df.shape[0]))
            yield df

    def get_contact_details_from_contactids_job(self,contact_ids_list):
        '''
        :param company_ids_list:
        :return:
        '''
        #todo: only one job run at a time. maximum two possible at a time
        headers = {'accessToken':accessToken,'Accept':'application/json','Content-Type':'text/plain'}
        for contact_ids in chunker(contact_ids_list,9999):
            body = ','.join([str(i) for i in contact_ids])
            r_dic = self.post_job_and_get_jobid(url=contact_fetch_job_url,headers=headers,body=body)
            job_id = r_dic.get('jobId')
            if not job_id:
                raise ValueError('Some error happened, could not get job id from insideview')
            logging.info('waiting for contact details job_id:{},no of contacts to get details:{}'.format(job_id,len(contact_ids)))
            _ = self.wait_till_job_completes(url=contact_fetch_job_status_url.format(jobId=job_id),headers=headers)
            # getting results
            r = requests.get(contact_fetch_job_results_url.format(jobId=job_id),headers=headers)
            io_read = StringIO(r.text)
            df = pd.read_csv(io_read)
            logging.info('got results for job_id:{},no of records:{}'.format(job_id,df.shape[0]))
            yield df

    def search_contacts_from_company_ids(self,company_ids,max_res_per_company=5,**filters_dic):
        '''
        :param company_ids:
        :param kwargs:
        :return:
        '''
        # max_no_results=len(comp_ids)*(max_res_per_company+5)
        company_ids = [i for i in company_ids if i] # sometimes none coming
        all_contacts = []
        no_comps_to_process_single_iter = 500/(max_res_per_company) #
        for comp_ids in chunker(company_ids,no_comps_to_process_single_iter):
            comp_id_str = ','.join([str(i) for i in comp_ids])
            filters_dic['companyIdsToInclude'] = comp_id_str
            all_contacts.extend(self.search_contacts(max_no_results=no_comps_to_process_single_iter*50,**filters_dic))
        return all_contacts

    def search_contacts(self,max_no_results=9999,results_per_page=500,**kwargs):
        '''
        :param max_no_results: max no results needed
        :param results_per_page: how many result per page, max 500
        :param kwargs: filter options(all available options are given in the api ref page:
                    (https://kb.insideview.com/hc/en-us/articles/204395607--POST-Contact-List)
        :return:
        '''
        out_list = []
        search_dic = {'url':contact_search_url}
        kwargs['resultsPerPage'] = results_per_page
        total_results,res_page_no = 9999999,0
        logging.info('companyIdsToInclude:{}'.format(kwargs['companyIdsToInclude']))
        while min(max_no_results,total_results) > res_page_no*results_per_page:
            res_page_no += 1
            kwargs['page'] = res_page_no
            r = requests.post(self.throttler_app_address,params=search_dic,json=kwargs)
            self.api_counter.newcontact_search_hits += 1
            res_dic = json.loads(r.text)
            if res_dic.get('message') in ['request throttled by insideview','1000 per 5 minute']: #throttling reached, need to hit again after some time
                time.sleep(30)
                self.api_counter.newcontact_search_hits -= 1 #reduce the count
                res_page_no -= 1
                continue
            elif res_dic.get('message'):
                    raise ValueError('Error happened. {}'.format(res_dic))
            if not res_dic.get('contacts',None):
                break
            out_list.extend(res_dic.get('contacts'))
            total_results = int(res_dic['totalResults'])
            logging.info('total results:{},page no:{}'.format(total_results,res_page_no))
        logging.info('total_results:{},res_page_no:{},contacts_fetched:{}'.format(total_results,res_page_no,len(out_list)))
        return out_list

    def get_company_tech_profile_from_id(self,company_id):
        '''
        :param company_id:
        :return:
        '''
        raise NotImplemented
        # contact_url = company_tech_profile_url.format(companyId=company_id)
        # search_dic = {'url':contact_url}
        # r = requests.get(self.throttler_app_address,params=search_dic)
        # res_dic = json.loads(r.text)
        # return res_dic


    def get_contact_details_from_newcontact_id(self,new_contact_id,retrieve_comp_dets=1):
        '''
        :param new_contact_id:
        :param retrieve_comp_dets:
        :return:
        '''
        contact_url = contact_details_url.format(newcontactId=new_contact_id)
        search_dic = {'url':contact_url}
        if retrieve_comp_dets:
            search_dic['retrieveCompanyDetails'] = True
        r = requests.get(self.throttler_app_address,params=search_dic)
        res_dic = json.loads(r.text)
        return res_dic

    def search_insideview_contact(self,search_dic):
        ''' search insidevw with the parameters in search_dict
        :param search_dict:
        :return:
        '''
        search_dic['url'] = people_search_url
        search_dic['resultsPerPage'] = 50
        r = requests.get(self.throttler_app_address,params=search_dic)
        res_dic = json.loads(r.text)
        return res_dic