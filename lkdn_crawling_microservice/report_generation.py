__author__ = 'joswin'

import pandas as pd
from optparse import OptionParser
from postgres_connect import PostgresConnect
from constants import designations_column_name,desig_list_regex

#con object should have cursor enabled in all functions in this module

def get_count_input_company_name(list_id,con):
    '''no of input companies given for crawling
    :param list_id:
    :param con:
    :return:
    '''
    query = "select count((list_input_additional)) from crawler.list_items where list_id=%s"
    con.cursor.execute(query,(list_id,))
    res_list = con.cursor.fetchall()
    return res_list[0][0]

def get_count_lkdn_urls_found(list_id,con):
    ''' no of urls found by linkedin url finder
    :param list_id:
    :param con:
    :return:
    '''
    query = "select count(distinct url) from crawler.list_items_urls where list_id=%s"
    con.cursor.execute(query,(list_id,))
    res_list = con.cursor.fetchall()
    return res_list[0][0]

def get_count_lkdn_urls_not_found(list_id, con):
    ''' no of urls found by linkedin url finder
    :param list_id:
    :param con:
    :return:
    '''
    query = "select count(distinct (a.list_input,list_input_additional)) from crawler.list_items a left join crawler.list_items_urls b \
             on a.list_id=b.list_id and a.id=b.list_items_id where a.list_id = %s and b.url is  null"
    con.cursor.execute(query, (list_id,))
    res_list = con.cursor.fetchall()
    return res_list[0][0]

def get_count_company_crawled(list_id,con):
    ''' no of company pages crawled
    :param list_id:
    :param con:
    :return:
    '''
    query = "select count(distinct c.list_items_id) from " \
            "crawler.linkedin_company_base a join crawler.linkedin_company_redirect_url b on "\
            "(a.linkedin_url = b.redirect_url or a.linkedin_url = b.url) join " \
            "crawler.list_items_urls c on a.list_id=c.list_id and (b.redirect_url=c.url or b.url=c.url) "\
            " where a.list_id = %s"
    con.cursor.execute(query,(list_id,))
    res_list = con.cursor.fetchall()
    return res_list[0][0]
def get_count_valid_company_crawled(list_id, con):
    ''' no of company pages crawled
    :param list_id:
    :param con:
    :return:
    '''
    query = "select count(distinct c.list_items_id) from " \
            "crawler.linkedin_company_base a join crawler.linkedin_company_redirect_url b on " \
            "(a.linkedin_url = b.redirect_url or a.linkedin_url = b.url) join " \
            "crawler.list_items_urls c on a.list_id=c.list_id and (b.redirect_url=c.url or b.url=c.url) " \
            " where a.list_id = %s and a.isvalid =1"
    con.cursor.execute(query, (list_id,))
    res_list = con.cursor.fetchall()
    return res_list[0][0]


def get_count_inValid_company_crawled(list_id, con):
    ''' no of company pages crawled
    :param list_id:
    :param con:
    :return:
    '''
    query = "select count(distinct c.list_items_id) from " \
            "crawler.linkedin_company_base a join crawler.linkedin_company_redirect_url b on " \
            "(a.linkedin_url = b.redirect_url or a.linkedin_url = b.url) join " \
            "crawler.list_items_urls c on a.list_id=c.list_id and (b.redirect_url=c.url or b.url=c.url) " \
            " where a.list_id = %s and a.isvalid =0"
    con.cursor.execute(query, (list_id,))
    res_list = con.cursor.fetchall()
    return res_list[0][0]

def get_count_people_crawled_total(list_id,con):
    ''' total no of people pages crawled
    :param list_id:
    :param con:
    :return:
    '''
    query = "select count(distinct linkedin_url) from crawler.linkedin_people_base " \
            "where list_id = %s"
    con.cursor.execute(query,(list_id,))
    res_list = con.cursor.fetchall()
    return res_list[0][0]

def get_count_people_list_valid(res_df):
    ''' no of valid people and the no of companies for these people
    :param res_df: output pandas dataframe generated in save_output_to_file
    :return: (valid_company_count,valid_people_count)
    '''
    ppl_url_col,cmpny_url_col = 'people_linkedin_url','company_linkedin_url'
    return len(set(list(res_df[cmpny_url_col]))),len(set(list(res_df[ppl_url_col])))