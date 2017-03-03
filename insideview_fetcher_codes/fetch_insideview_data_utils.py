__author__ = 'joswin'
# -*- coding: utf-8 -*-

import csv
import logging
import hashlib
import pandas as pd
from postgres_connect import PostgresConnect
from random import shuffle

from constants import database,host,user,password,designations_column_name

class InsideviewDataUtil(object):
    '''
    '''
    def __init__(self):
        self.con = PostgresConnect(database_in=database,host_in=host,
                                                    user_in=user,password_in=password)

    def get_designations(self,desig_loc):
        '''
        :param desig_loc:
        :return:
        '''
        if desig_loc:
            inp_df = pd.read_csv(desig_loc)
            desig_list = list(inp_df[designations_column_name])
        else:
            desig_list = []
        return desig_list

    def save_company_search_res_single(self,list_id,list_items_id,res_list):
        '''
        :param list_id:
        :param list_items_id:
        :param res_list:
        :return:
        '''
        res_to_insert = [
            (list_id,list_items_id,res.get('city',None),res.get('state',None),res.get('country',None),
                res.get('name',None),res.get('companyId',None))
            for res in res_list
        ]
        records_list_template = ','.join(['%s'] * len(res_to_insert))
        insert_query = 'insert into crawler.insideview_company_search_res ' \
                       ' (list_id, list_items_id,city,state,country,name,company_id) values {}'.format(records_list_template)
        self.con.execute(insert_query, res_to_insert)
        self.con.commit()

    def get_companies_for_contact_search(self,list_id,comp_contries_loc,find_new_companies_only=1,
                                         comp_ids_to_find_contacts_file_loc=None):
        ''' get the list of companies for which the insideview contact search will be done '''
        if comp_ids_to_find_contacts_file_loc:
            df = pd.read_csv(comp_ids_to_find_contacts_file_loc)
            df = df[~pd.isnull(df['company_id'])]
            comp_ids = list(set(df['company_id']))
            query = " select distinct company_id from crawler.insideview_contact_search_res where " \
                        " list_id=%s and company_id in %s"
            self.con.execute(query,(list_id,tuple(comp_ids),))
            comp_ids_present = self.con.cursor.fetchall()
            comp_ids_present = [i[0] for i in comp_ids_present]
            comp_ids_not_present = list(set(comp_ids)-set(comp_ids_present))
            if comp_ids_not_present:
                query = " select distinct company_id from crawler.insideview_company_search_res where " \
                        " list_id = %s and company_id in %s"
                self.con.execute(query,(list_id,tuple(comp_ids_not_present),))
                comp_ids_present = self.con.cursor.fetchall()
                comp_ids_present = [i[0] for i in comp_ids_present]
                comp_ids_not_present_in_comp_search = list(set(comp_ids_not_present)-set(comp_ids_present))
                if comp_ids_not_present_in_comp_search:
                    raise ValueError('Some company ids are not present in the company search results. eg:{}'.format(
                        comp_ids_not_present_in_comp_search[0]
                    ))
            if find_new_companies_only:
                return comp_ids_not_present
            else:
                return comp_ids
        else:
            # todo: fix this logic - this logic might be wrong.
            # find_new_contacts_only = 0
            if find_new_companies_only:
                query = "select distinct a.company_id from crawler.insideview_company_search_res a left join " \
                        " crawler.insideview_contact_search_res b on a.list_id=b.list_id and a.company_id=b.company_id " \
                        " where b.company_id is null and a.list_id=%s"
            else:
                query = "select distinct company_id from crawler.insideview_company_search_res a " \
                        " where list_id=%s "
            # if country based filters available, apply them
            if comp_contries_loc:
                countries = self.get_contries_list(comp_contries_loc)
                query = query + ' and a.country in %s'
                self.con.execute(query,(list_id,tuple(countries),))
            else:
                self.con.execute(query,(list_id,))
            comp_ids = self.con.cursor.fetchall()
            comp_ids = [i[0] for i in comp_ids]
            return comp_ids

    def get_company_ids_missing(self,list_id,comp_ids):
        '''get company details from insideview and save the result for each company id in the list '''
        logging.info('started get_save_company_details_from_insideview_listinput')
        # find all company ids not present in the company_details table
        # todo: can add a timestamp related filter here later
        query = " select distinct a.company_id from crawler.insideview_company_search_res a " \
                    " left join crawler.insideview_company_details_contact_search b on a.company_id=b.company_id " \
                    " where a.list_id=%s and a.company_id in %s and b.company_id is null"
        self.con.execute(query,(list_id,tuple(comp_ids),))
        # comp_ids_already_present = self.con.cursor.fetchall()
        # comp_ids_already_present = [i[0] for i in comp_ids_already_present]
        # comp_ids_not_preset = list(set(comp_ids)-set(comp_ids_already_present))
        comp_ids_not_present = self.con.cursor.fetchall()
        comp_ids_not_present = [i[0] for i in comp_ids_not_present]
        return comp_ids_not_present

    def get_dets_for_insideview_fetch(self,list_id,remove_comps_in_lkdn_table=0,max_comps_to_try=0):
        '''find the company names and urls which needs to be fetched from insideview
        '''
        # todo: we can add a flag in list_items/list_items_urls table and use it here to avoid duplication
        # find the companies for whom the search was not done in builtwith
        query = " select a.company_name,a.website,a.country,a.state,a.city,a.id from " \
                " crawler.list_items_insideview_companies a left join " \
                " crawler.insideview_company_search_res b on a.list_id=b.list_id and a.id=b.list_items_id " \
                " where a.list_id = %s and b.list_id is null"
        self.con.execute(query,(list_id,))
        comp_input_dets_no_iv = self.con.cursor.fetchall()
        # find all company details not present in linkedin_company_base if remove_comps_in_lkdn_table flag is True
        # if remove_comps_in_lkdn_table:
        #     query = "select distinct list_input,list_input_additional,a.id from crawler.list_items a  left join " \
        #             " crawler.list_items_urls b on a.id=b.list_items_id and a.list_id=b.list_id left join crawler.linkedin_company_base c " \
        #             " on b.id=c.list_items_url_id and a.list_id=c.list_id where a.list_id = %s and (b.url is null or c.linkedin_url is null)"
        #     self.con.execute(query,(list_id,))
        #     comp_input_dets_no_lkdn = self.con.cursor.fetchall()
        # else:
        #     comp_input_dets_no_lkdn = comp_input_dets_no_iv #this is done coz of the intersection below
        # find intersection
        # comp_input_dets = list(set(comp_input_dets_no_lkdn).intersection(set(comp_input_dets_no_iv)))
        comp_input_dets = comp_input_dets_no_iv
        shuffle(comp_input_dets)
        # try for max_comps_to_try at a time if max_comps_to_try present
        if max_comps_to_try:
            comp_input_dets = comp_input_dets[:min(max_comps_to_try,len(comp_input_dets))]
        return comp_input_dets

    def gen_filters_dic(self,filters_loc):
        '''saved as key,value in each row. this will be passed to the api filter search
        :param filters_loc: location of file
        :return:
        '''
        filter_dic = {}
        if not filters_loc:
            return filter_dic
        rows = []
        with open(filters_loc,'r') as f:
            reader = csv.reader(f)
            for row in reader:
                rows.append(row)
        for row in rows:
            key,value = row
            try:
                value_tmp = eval(value)
                if not type(value_tmp) == tuple:
                    value = value_tmp
            except:
                pass
            filter_dic[key] = value
        return filter_dic

    def get_contries_list(self,comp_contries_loc):
        '''
        :param comp_contries_loc:location of csv with country names
        :return:
        '''
        df = pd.read_csv(comp_contries_loc)
        return list(df['countries'])

    def save_contacts_seach_res(self,list_id,res_list):
        '''
        :param list_id:
        :param res_list:
        :return:
        '''
        # res_to_insert = [
        #     (list_id,res['firstName'],res['lastName'],res['fullName'],res['id'],res['peopleId'],res['titles'],
        #      res['active'],res['companyId'],res['companyName'],res['hasEmail'],res['emailMd5Hash'],res['hasPhone'],
        #      res['city'],res['state'],res['country'])
        #     for res in res_list
        # ]
        if res_list:
            res_to_insert = [
                (list_id,res.get('firstName',None),res.get('lastName',None),res.get('fullName',None),res.get('id',None),
                    res.get('peopleId',None),res.get('titles',None),res.get('active',None),res.get('companyId',None),
                    res.get('companyName',None),res.get('hasEmail',None),res.get('emailMd5Hash',None),
                    res.get('hasPhone',None),res.get('city',None),res.get('state',None),res.get('country',None))
                for res in res_list
            ]
            records_list_template = ','.join(['%s'] * len(res_to_insert))
            insert_query = 'insert into crawler.insideview_contact_search_res ' \
                           ' (list_id, first_name,last_name,full_name,new_contact_id,people_id,titles,active,company_id,' \
                           'company_name,has_email,email_md5_hash,has_phone,city,state,country) values' \
                           ' {} ON CONFLICT DO NOTHING'.format(records_list_template)
            self.con.execute(insert_query, res_to_insert)
            self.con.commit()

    def save_contact_info(self,res_dic):
        '''
        :param list_id:
        :param res_dic:
        :return:
        '''
        # generating md5 hash of email
        md5_hasher = hashlib.md5()
        if res_dic.get('email',None):
            md5_hasher.update(res_dic.get('email',None))
            email_md5_hash = md5_hasher.hexdigest()
        else:
            email_md5_hash = None
        insert_column_template = ','.join(['%s']*23) #23 columns to insert
        insert_query = "insert into crawler.insideview_contact_data " \
                       " (contact_id,people_id,active,first_name,last_name,full_name,titles,company_id, " \
                       " company_name,age,description,email,email_md5_hash,job_function,job_levels,phone,salary, " \
                       " salary_currency,image_url,facebook_url,linkedin_url,twitter_url,sources) values " \
                       " ( {} ) ".format(insert_column_template)
        res_to_insert = (res_dic.get('contactId',None),res_dic.get('peopleId',None),res_dic.get('active',None),
                         res_dic.get('firstName',None),res_dic.get('lastName',None),res_dic.get('fullName',None),
                         res_dic.get('titles',None),res_dic.get('companyId',None),res_dic.get('companyName',None),
                         res_dic.get('age',None),res_dic.get('description',None),res_dic.get('email',None),email_md5_hash,
                         res_dic.get('jobFunctions',None),res_dic.get('jobLevels',None),
                         res_dic.get('phone',None),res_dic.get('salary',None),res_dic.get('salaryCurrency',None),
                         res_dic.get('imageUrl',None),res_dic.get('facebookHandle',None),
                         res_dic.get('linkedinHandle',None),res_dic.get('twitterHandle',None),
                         res_dic.get('sources',None),)
        self.con.execute(insert_query,res_to_insert)
        self.con.commit()
        self.save_contact_info_misc(contact_id=res_dic.get('contactId',None),people_id=res_dic.get('peopleId',None),
                                    education=res_dic.get('education',[]))
        if 'companyDetails' in res_dic:
            self.save_company_dets_dic_input(res_dic['companyDetails'])

    def save_contact_info_misc(self,contact_id,people_id,education=[]):
        ''' json columns returned are saved separately
        :param contact_id:
        :param people_id:
        :param education:
        :return:
        '''
        # inserting british sics data
        education_query = 'insert into crawler.insideview_contact_education ' \
                             ' (contact_id,people_id,degree,major,university) values ' \
                             ' (%s,%s,%s,%s,%s) '
        for det_dic in education:
            if det_dic:
                degree,major,university = det_dic.get('degree',None),det_dic.get('major',None),det_dic.get('university',None)
                self.con.execute(education_query,(contact_id,people_id,degree,major,university,))

        self.con.commit()

    def save_company_dets_dic_input(self,res_dic):
        '''
        :param list_id:
        :param res_dic:
        :return:
        '''
        insert_column_template = ','.join(['%s']*46) #46 columns to insert
        insert_query = "insert into crawler.insideview_company_details_contact_search " \
                       " (company_id,company_status,company_type,name,websites,subsidiary,parent_company_id," \
                       "parent_company_name,parent_company_country,industry,industry_code,sub_industry," \
                       "sub_industry_code,sic,sic_description,naics,naics_description,employees,employee_range," \
                       "fortune_ranking,foundation_date,gender,ethnicity,dbe,wbe,mbe,vbe,disabled,lgbt,revenue," \
                       "revenue_currency,revenue_range,most_recent_quarter,financial_year_end,phone,fax,street,city,state,country," \
                       "zip,equifax_id,ultimate_parent_company_id,ultimate_parent_company_name," \
                       "ultimate_parent_company_country,sources) values " \
                       " ( {} ) ".format(insert_column_template)
        res_to_insert = (res_dic.get('companyId',None),res_dic.get('companyStatus',None),
                         res_dic.get('companyType',None),res_dic.get('name',None),res_dic.get('websites',None),
                         res_dic.get('subsidiary',None),res_dic.get('parentCompanyId',None),
                         res_dic.get('parentCompanyName',None),res_dic.get('parentCompanyCountry',None),
                         res_dic.get('industry',None),res_dic.get('industryCode',None),res_dic.get('subIndustry',None),
                         res_dic.get('subIndustryCode',None),res_dic.get('sic',None),res_dic.get('sicDescription',None),
                         res_dic.get('naics',None),res_dic.get('naicsDescription',None),
                         res_dic.get('employees',None),res_dic.get('employeeRange',None),res_dic.get('fortuneRanking',None),
                         res_dic.get('foundationDate',None),res_dic.get('gender',None),res_dic.get('ethnicity',None),
                         res_dic.get('dbe',None),res_dic.get('wbe',None),res_dic.get('mbe',None),
                         res_dic.get('vbe',None),res_dic.get('disabled',None),res_dic.get('lgbt',None),
                         res_dic.get('revenue',None),res_dic.get('revenueCurrency',None),res_dic.get('revenueRange',None),
                         res_dic.get('mostRecentQuarter',None),res_dic.get('financialYearEnd',None),
                         res_dic.get('phone',None),res_dic.get('fax',None),res_dic.get('street',None),
                         res_dic.get('city',None),res_dic.get('state',None),res_dic.get('country',None),
                         res_dic.get('zip',None),res_dic.get('equifaxId',None),
                         res_dic.get('ultimateParentId',None),res_dic.get('ultimateParentCompanyName',None),
                         res_dic.get('ultimateParentCompanyCountry',None),res_dic.get('sources',None),)
        self.con.execute(insert_query,res_to_insert)
        self.con.commit()
        # save ticker and britishSics separately
        self.save_company_dets_misc(res_dic.get('companyId',None),res_dic.get('britishSics',[]),
                                    res_dic.get('tickers',[]))

    def save_company_dets_misc(self,company_id,british_sics=[],tickers=[]):
        '''
        :param british_sics: dictionary
        :param tickers: dictionary
        :return:
        '''
        # inserting british sics data
        british_sics_query = 'insert into crawler.insideview_company_british_sics ' \
                             ' (company_id,british_sic,description) values ' \
                             ' (%s,%s,%s) '
        for det_dic in british_sics:
            sic_value,description = det_dic.get('britishSic',None),det_dic.get('description',None)
            self.con.execute(british_sics_query,(company_id,sic_value,description,))
        # inserting tickers data
        tickers_query = ' insert into crawler.insideview_company_tickers ' \
                        ' (company_id,ticker_name,exchange) values (%s,%s,%s)'
        for det_dic in tickers:
            ticker_name,exchange = det_dic.get('tickerName',None),det_dic.get('exchange',None)
            self.con.execute(tickers_query,(company_id,ticker_name,exchange,))
        self.con.commit()

    def expand_tech_profile_dic(self,company_id,res_dic):
        ''' techprofile dictionary is in nested format. expand it so that it can be saved to table
        :param company_id:
        :param res_dic:
        :return:
        '''
        out_list = []
        for category_dic in res_dic.get('categories',[]):
            category_name = category_dic.get('categoryName',None)
            category_id = category_dic.get('categoryId',None)
            for sub_category_dic in category_dic.get('subCategories',[]):
                sub_category_id = sub_category_dic.get('subCategoryId',None)
                sub_category_name = sub_category_dic.get('subCategoryName',None)
                for product_dic in sub_category_dic.get('products',[]):
                    product_name = product_dic['productName']
                    product_id = product_dic['productId']
                    out_list.append((company_id,category_id,category_name,sub_category_id,sub_category_name,
                                        product_id,product_name))
        return out_list

    def save_company_techs(self,company_id,res_dic):
        '''
        :param company_id:
        :param res_dic:this will be the output of  get_company_tech_profile_from_id function
        :return:
        '''
        res_list = self.expand_tech_profile_dic(company_id,res_dic)
        records_list_template = ','.join(['%s'] * len(res_list))
        insert_query = 'insert into crawler.insideview_company_tech_details ' \
                       ' (company_id,category_id,category_name,sub_category_id,sub_category_name,' \
                       ' product_id,product_name) values' \
                       ' {}'.format(records_list_template)
        self.con.execute(insert_query, res_list)
        self.con.commit()

    def get_new_contactids_for_email_find(self,list_id,comp_ids,max_res_per_company=3,desig_list=[],
                                          new_contact_ids_file_loc=None):
        '''
        :param list_id:
        :param comp_ids:
        :param max_res_per_company:
        :param desig_list:
        :param new_contact_ids_file_loc:
        :return:
        '''
        if new_contact_ids_file_loc:
            df = pd.read_csv(new_contact_ids_file_loc)
            new_contact_ids = list(set(df['new_contact_id'])) #should check in people table to see if email already present?
            query = "select distinct a.new_contact_id from crawler.insideview_contact_search_res a " \
                    " join crawler.insideview_contact_data b on a.email_md5_hash=b.email_md5_hash " \
                    " where a.new_contact_id in %s "
            self.con.cursor.execute(query,(tuple(new_contact_ids),))
            new_contact_ids_in_db = self.con.cursor.fetchall()
            new_contact_ids_in_db = [i[0] for i in new_contact_ids_in_db]
            new_contact_ids = list(set(new_contact_ids)-set(new_contact_ids_in_db))
        else:
            if not comp_ids:
                return []
            # get contact ids which are not present in the contacts table
            if desig_list:
                desig_list_reg = '\y' + '\y|\y'.join(desig_list) + '\y'
                query = "SELECT distinct new_contact_id FROM " \
                        " (SELECT company_id,new_contact_id, ROW_NUMBER() OVER (PARTITION BY company_id ) AS Row_ID FROM " \
                        " ( select distinct a.company_id,a.new_contact_id from " \
                        " crawler.insideview_contact_search_res a left join crawler.insideview_contact_data b " \
                        " on a.email_md5_hash=b.email_md5_hash where a.list_id = %s and a.company_id in %s and " \
                        " a.has_email = 't' and b.email_md5_hash is null " \
                        " and array_to_string(a.titles,',') ~* '{}' )x "\
                        "  ) as A " \
                        " WHERE Row_ID <= {} ".format(desig_list_reg,max_res_per_company)
            else:
                query = "SELECT distinct new_contact_id FROM " \
                        " (SELECT company_id,new_contact_id, ROW_NUMBER() OVER (PARTITION BY company_id ) AS Row_ID FROM " \
                        " ( select distinct a.company_id,a.new_contact_id from " \
                        " crawler.insideview_contact_search_res a left join crawler.insideview_contact_data b " \
                        " on a.email_md5_hash=b.email_md5_hash where a.list_id = %s and a.company_id in %s and " \
                        " a.has_email = 't' and b.email_md5_hash is null )x"\
                        "  ) as A " \
                        " WHERE Row_ID <= {} ".format(max_res_per_company)
            self.con.cursor.execute(query,(list_id,tuple(comp_ids),))
            new_contact_ids = self.con.cursor.fetchall()
            new_contact_ids = [i[0] for i in new_contact_ids]
        return new_contact_ids

    def get_contact_ids_for_email_find(self,list_id,comp_ids,max_res_per_company=5,desig_list=[],
                                          contact_ids_file_loc=None):
        '''
        :param list_id:
        :param comp_ids:
        :param desig_list:
        :param new_contact_ids_file_loc:
        :return:
        '''
        if contact_ids_file_loc:
            df = pd.read_csv(contact_ids_file_loc)
            if 'contact_id' not in df:
                return []
            contact_ids = list(set(df['contact_id'])) #should check in people table to see if email already present?
            query = "select distinct contact_id from crawler.insideview_contact_data a "\
                    " where a.contact_id in %s "
            self.con.cursor.execute(query,(tuple(contact_ids),))
            contact_ids_in_db = self.con.cursor.fetchall()
            contact_ids_in_db = [i[0] for i in contact_ids_in_db]
            contact_ids = list(set(contact_ids)-set(contact_ids_in_db))
            return contact_ids
        else:
            if not comp_ids:
                return []
            # below query gets people contactids who are missing in the final contact table
            if desig_list:
                desig_list_reg = '\y' + '\y|\y'.join(desig_list) + '\y'
                query = "SELECT distinct contact_id FROM " \
                        " (SELECT company_id,contact_id, ROW_NUMBER() OVER (PARTITION BY company_id ) AS Row_ID FROM " \
                        "(" \
                        "SELECT distinct a.company_id,a.contact_id FROM crawler.insideview_contact_name_search_res a " \
                        " join crawler.insideview_contact_search_res c on a.list_id=c.list_id and " \
                        " a.input_name_id=c.people_id and a.company_id=c.company_id and a.first_name=c.first_name and a.last_name=c.last_name " \
                        " left join crawler.insideview_contact_data b on a.contact_id = b.contact_id " \
                        " where a.list_id = %s and a.has_email = 't' and b.contact_id is null and a.active='t' " \
                        " and a.company_id in %s "\
                        " and array_to_string(a.titles,',') ~* '{regex}' " \
                        " ) as x"\
                        "  ) as A " \
                        "WHERE Row_ID <= {max_res}".format(regex=desig_list_reg,max_res=max_res_per_company)
            else:
                # raise ValueError('Need designation list')
                query = "SELECT distinct contact_id FROM " \
                        " (SELECT company_id,contact_id, ROW_NUMBER() OVER (PARTITION BY company_id ) AS Row_ID FROM " \
                        "(" \
                        "SELECT distinct a.company_id,a.contact_id FROM crawler.insideview_contact_name_search_res a " \
                        " join crawler.insideview_contact_search_res c on a.list_id=c.list_id and " \
                        " a.input_name_id=c.people_id and a.company_id=c.company_id and a.first_name=c.first_name and a.last_name=c.last_name " \
                        " left join crawler.insideview_contact_data b on a.contact_id = b.contact_id " \
                        " where a.list_id = %s and a.has_email = 't' and b.contact_id is null and a.active='t' " \
                        " and a.company_id in %s" \
                        " ) as x"\
                        "  ) as A " \
                        " WHERE Row_ID <= {} ".format(max_res_per_company)
            self.con.cursor.execute(query,(list_id,tuple(comp_ids),))
            contact_ids = self.con.cursor.fetchall()
            contact_ids = [i[0] for i in contact_ids]
            return contact_ids

    def get_people_details_for_email_find(self,list_id,comp_ids,desig_list=[],
                                          people_details_file=None):
        '''used while seaching for contacts from name and details
        :param list_id:
        :param comp_ids:
        :param max_res_per_company:
        :param desig_list:
        :return:
        '''
        # note: whenever the return order is changed to add more details,change should get reflected in
        # the functions that use this function. eg: search_for_matching_people_from_ppl_details in company fetching part
        if people_details_file:
            # todo : this is not working properly. so need to make this better
            df = pd.read_csv(people_details_file)
            df = df.fillna('')
            if 'company_id' not in df:
                return []
            people_details = []
            for index,row in df.iterrows():
                if row['people_id']:
                    # check if the person is not already searched- earlier used name and company_id, change it to people id
                    query = " select id from crawler.insideview_contact_name_search_res where list_id=%s and " \
                            " input_name_id=%s"
                    self.con.cursor.execute(query,(list_id,row['people_id'],))
                    person_search_id = self.con.cursor.fetchall()
                    if not person_search_id:
                        # check if the person is present in the contact_search table. if not, raise error
                        query = " select id from crawler.insideview_contact_search_res where list_id=%s and " \
                                " people_id=%s "
                        self.con.cursor.execute(query,(list_id,row['people_id'],))
                        res = self.con.cursor.fetchall()
                        if not res:
                            raise ValueError('A people_id is not part of the contact search res for this list_name.'
                                             'please check the input. problematic people_id:{}'.format(row['people_id']))
                        # the person is not searched already, so add it to the list of persons to search
                        people_details.append(tuple(row[['company_id','first_name','last_name','full_name','people_id']]))
                else:
                    raise ValueError('Need people_id for all inputs. Please check the input file')
            return people_details
        else:
            if not comp_ids:
                return []
            if desig_list:
                desig_list_reg = '\y' + '\y|\y'.join(desig_list) + '\y'
                query = " select distinct a.company_id,a.first_name,a.last_name,a.full_name,a.people_id from " \
                        " crawler.insideview_contact_search_res a left join crawler.insideview_contact_data b " \
                        " on a.email_md5_hash=b.email_md5_hash " \
                        " left join crawler.insideview_contact_name_search_res c on " \
                        " a.people_id=c.input_name_id " \
                        " where a.list_id = %s and a.company_id in %s and " \
                        " a.has_email = 't' and b.email_md5_hash is null and c.input_name_id is null " \
                        " and array_to_string(a.titles,',') ~* '{}' ".format(desig_list_reg)
            else:
                query = " select distinct a.company_id,a.first_name,a.last_name,a.full_name,a.people_id from " \
                        " crawler.insideview_contact_search_res a left join crawler.insideview_contact_data b " \
                        " on a.email_md5_hash=b.email_md5_hash " \
                        " left join crawler.insideview_contact_name_search_res c on " \
                        " a.people_id=c.input_name_id " \
                        " where a.list_id = %s and a.company_id in %s and " \
                        " a.has_email = 't' and b.email_md5_hash is null and c.input_name_id is null "
            self.con.cursor.execute(query,(list_id,tuple(comp_ids),))
            people_details = self.con.cursor.fetchall()
            return people_details

    def save_contact_search_res_single(self,list_id,res_list,person_id):
        '''
        :param list_id:
        :param res_list:
        :return:
        '''
        if res_list:
            res_to_insert = [
                (list_id,res.get('firstName',None),res.get('middleName',None),res.get('lastName',None),
                    res.get('contactId',None),res.get('companyId',None),res.get('companyName',None),res.get('titles',None),
                    res.get('active',None),res.get('hasEmail',None),res.get('hasPhone',None),res.get('peopleId',None),
                 person_id
                )
                for res in res_list
            ]
            records_list_template = ','.join(['%s'] * len(res_to_insert))
            insert_query = 'insert into crawler.insideview_contact_name_search_res ' \
                           ' (list_id,first_name ,middle_name ,last_name ,contact_id ,company_id , ' \
                           ' company_name ,titles ,active ,has_email,has_phone ,people_id,input_name_id)' \
                           ' values {}'.format(records_list_template)
            self.con.execute(insert_query, res_to_insert)
            self.con.commit()

    def get_new_contact_ids_to_fetch(self,list_id,comp_ids,max_res_per_company=3,
                                              desig_list=[],
                                              new_contact_ids_file_loc=None):
        '''This will give the new contactids that need to be fetched
        :param list_id:
        :param comp_ids:
        :param max_res_per_company: max number of email searches to be done for each company
        :param filters_dic:
        :return:
        '''
        logging.info('started fetch_people_details_from_company_ids_crawler_process')
        if new_contact_ids_file_loc:
            df = pd.read_csv(new_contact_ids_file_loc)
            if 'new_contact_id' not in df:
                return []
            new_contact_ids = list(set(df['new_contact_id'])) #should check in people table to see if email already present?
            new_contact_ids = [i for i in new_contact_ids if i]
            query = "select distinct b.new_contact_id from crawler.insideview_contact_data a join " \
                    " crawler.insideview_contact_search_res b on a.email_md5_hash=b.email_md5_hash " \
                    " where b.new_contact_id in %s and  b.list_id=%s"
            self.con.cursor.execute(query,(tuple(new_contact_ids),list_id,))
            new_contact_ids_in_db = self.con.cursor.fetchall()
            new_contact_ids_in_db = [i[0] for i in new_contact_ids_in_db]
            new_contact_ids = list(set(new_contact_ids)-set(new_contact_ids_in_db))
            return new_contact_ids
        else:
            # get contact ids which are not present in the contacts table
            if desig_list:
                desig_list_reg = '\y' + '\y|\y'.join(desig_list) + '\y'
                query = "SELECT distinct new_contact_id FROM " \
                        " (SELECT company_id,new_contact_id, ROW_NUMBER() OVER (PARTITION BY company_id ) AS Row_ID FROM " \
                        " ( select a.company_id,a.new_contact_id from " \
                        " crawler.insideview_contact_search_res a left join crawler.insideview_contact_data b " \
                        " on a.email_md5_hash=b.email_md5_hash where a.list_id = %s and a.company_id in %s and " \
                        " b.email_md5_hash is null and array_to_string(a.titles,',') ~* '{}' )x "\
                        "  ) as A " \
                        " WHERE Row_ID <= {} ".format(desig_list_reg,max_res_per_company)
            else:
                query = "SELECT distinct new_contact_id FROM " \
                        " (SELECT company_id,new_contact_id, ROW_NUMBER() OVER (PARTITION BY company_id ) AS Row_ID FROM " \
                        " ( select a.company_id,a.new_contact_id from " \
                        " crawler.insideview_contact_search_res a left join crawler.insideview_contact_data b " \
                        " on a.email_md5_hash=b.email_md5_hash where a.list_id = %s and a.company_id in %s and " \
                        " b.email_md5_hash is null )x"\
                        "  ) as A " \
                        " WHERE Row_ID <= {} ".format(max_res_per_company)
            self.con.cursor.execute(query,(list_id,tuple(comp_ids),))
            new_contact_ids = self.con.cursor.fetchall()
            new_contact_ids = [i[0] for i in new_contact_ids]
            return new_contact_ids


