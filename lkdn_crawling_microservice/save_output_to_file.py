# -*- coding: utf-8 -*-
__author__ = 'joswin'

import pandas as pd
import report_generation
from optparse import OptionParser
from postgres_connect import PostgresConnect
from constants import designations_column_name,desig_list_regex
from pandas import ExcelWriter
#import xlsxwriter

ppl_table_fields = ['input_website','input_company_name','id', 'list_id', 'list_items_url_id', 'full_name',
                    'first_name', 'middle_name', 'last_name',
                    'domain', 'designation', 'company_name', 'company_website', 'headquarters', 'location_person',
                    'industry', 'company_size', 'founded', 'company_linkedin_url', 'people_linkedin_url', 'created_on']

def changeencode(data):
    ''' util function for solving encoding errors
    :param data:
    :return:
    '''
    cols = data.columns
    for col in cols:
        if type(data.ix[0,col]) == str:
            # data[col] = data[col].apply(lambda x: str(unicode(x,'ascii','ignore')))
            # data[col] = data[col].apply(lambda x: unicode(x,'utf-8','ignore'))
            data[col] = data[col].str.decode('iso-8859-1').str.encode('utf-8')
            # data[col] = data[col].str.decode('utf-8','ignore').str.encode('utf-8')
    return data

def save_to_excel(res_df,report_df,out_loc):
    '''
    :param res_df:
    :param report_df:
    :param out_loc:
    :return:
    '''
    res_df = changeencode(res_df)
    # report_df = changeencode(report_df)
    writer = ExcelWriter(out_loc+'.xls',options={'encoding':'utf-8'})
    res_df.to_excel(writer,sheet_name='people_details',index=False)
    report_df.to_excel(writer,sheet_name='report',index=False)
    writer.save()

def save_to_csv(res_df,report_df,out_loc):
    '''
    :param res_df:
    :param report_df:
    :param out_loc:
    :return:
    '''
    res_df.to_csv(out_loc+'.csv',index=False,encoding='utf-8',quoting=1)
    report_df.to_csv(out_loc+'_report.csv',index=False,encoding='utf-8',quoting=1)

def save_res_to_file_auto_format(res_df,report_df,out_loc):
    '''
    :param res_df:
    :param report_df:
    :param out_loc:
    :return:
    '''
    out_loc = out_loc.split('.xls')[0]
    out_loc = out_loc.split('.csv')[0]
    try:
        save_to_excel(res_df,report_df,out_loc)
    except:
        save_to_csv(res_df,report_df,out_loc)

def get_data_from_table(list_id,desig_list_reg):
    '''
    :param list_id:
    :param desig_list_reg:
    :return:
    '''
    con = PostgresConnect()
    con.get_cursor()
    query = "create temp table if not exists %s as select distinct on (a.first_name,a.middle_name,a.last_name,a.domain) " \
        " d.list_input as input_website,d.list_input_additional input_company_name, "\
        " a.* from crawler.people_details_for_email_verifier_new a join crawler.linkedin_company_redirect_url b on " \
        " (a.company_linkedin_url = b.redirect_url or a.company_linkedin_url = b.url) " \
        " join crawler.list_items_urls c on (b.redirect_url=c.url or b.url=c.url) " \
        " join crawler.list_items d on c.list_id=d.list_id and c.list_items_id=d.id "\
        " where c.list_id = %s and a.domain is not null and designation ~* %s "
    print(query)
    print(desig_list_reg)
    con.cursor.execute(query,(list_id,desig_list_reg,))
    res_list = con.cursor.fetchall()
    con.close_cursor()
    con.close_connection()
    return res_list

def get_report_for_list(list_id,res_df):
    '''
    :param list_id:
    :param res_df:
    :return: dataframe with report
    '''
    con = PostgresConnect()
    con.get_cursor()
    inp_cnt = report_generation.get_count_input_company_name(list_id,con)
    urls_found = report_generation.get_count_lkdn_urls_found(list_id,con)
    companies_crawled = report_generation.get_count_company_crawled(list_id,con)
    total_people_crawled = report_generation.get_count_people_crawled_total(list_id,con)
    comps_with_valid_ppl,valid_ppl = report_generation.get_count_people_list_valid(res_df)

    urls_not_found = report_generation.get_count_lkdn_urls_not_found(list_id, con)
    valid_companies_crawled = report_generation.get_count_valid_company_crawled(list_id, con)

    return pd.DataFrame(
        [[inp_cnt, urls_found, companies_crawled, total_people_crawled, valid_ppl, comps_with_valid_ppl, urls_not_found
             , valid_companies_crawled]],
        columns=['input_count', 'lkdn_urls_found', 'lkdn_cmp_pages_crawled',
                 'lkdn_ppl_pages_crawled', 'valid_ppl_found', 'cmps_with_valid_ppl', 'linkedin_url_not_found',
                 'valid_linkedin_companies_crawled_count'])


def get_result_and_report(list_id,desig_list_reg):
    '''
    :param list_id:
    :param desig_list_reg:
    :param out_loc:
    :return:
    '''
    res_list = get_data_from_table(list_id,desig_list_reg)
    df = pd.DataFrame.from_records(res_list)
    df.columns = ppl_table_fields
    report_df = get_report_for_list(list_id,df)
    return df,report_df

def save_to_file(list_name,desig_loc=None,out_loc='people_details_extracted.csv'):
    '''
    :param list_name:
    :param out_loc:
    :return:
    '''
    con = PostgresConnect()
    query = 'select id from {} where list_name = %s'.format('crawler.list_table')
    con.get_cursor()
    con.cursor.execute(query,(list_name,))
    res_list = con.cursor.fetchall()
    con.close_cursor()
    con.close_connection()
    if not res_list:
        raise ValueError('Give correct list name')
    else:
        list_id = res_list[0][0]
    if desig_loc:
        inp_df = pd.read_csv(desig_loc)
        desig_list = list(inp_df[designations_column_name])
    else:
        desig_list = None
    if not desig_list:
        # desig_list_reg = desig_list_regex
        raise ValueError('Need designation list file')
    else:
        desig_list_reg = '\y' + '\y|\y'.join(desig_list) + '\y'
    df,report_df = get_result_and_report(list_id,desig_list_reg)
    save_res_to_file_auto_format(df,report_df,out_loc)

def save_to_file_batch(list_name,desig_loc=None,out_loc='people_details_extracted.csv',sep_files=True):
    '''
    :param list_name:
    :param desig_loc:
    :param out_loc:
    :param sep_files:if True, output will be saved in separate files for each batch, else a single file
    :return:
    '''
    out_loc = out_loc.split('.csv')[0] #for appending
    list_name_like = list_name+'%'
    con = PostgresConnect()
    query = 'select list_name,id from {} where list_name like %s'.format('crawler.list_table')
    con.get_cursor()
    con.cursor.execute(query,(list_name_like,))
    res_list = con.cursor.fetchall()
    con.close_cursor()
    con.close_connection()
    if not res_list:
        raise ValueError('Give correct list name')
    if desig_loc:
        inp_df = pd.read_csv(desig_loc)
        desig_list = list(inp_df[designations_column_name])
    else:
        desig_list = None
    if not desig_list:
        # desig_list_reg = desig_list_regex
        raise ValueError('Need designation list file')
    else:
        desig_list_reg = '\y' + '\y|\y'.join(desig_list) + '\y'

    res_df_combined,report_df_combined = [],[]
    for batch_list_name,batch_list_id in res_list:
        df,report_df = get_result_and_report(batch_list_id,desig_list_reg)
        if sep_files:
            out_loc_batch = '{}_{}.xls'.format(out_loc,batch_list_name)
            save_res_to_file_auto_format(df,report_df,out_loc_batch)
        else:
            res_df_combined.append(df)
            report_df['list_name'] = batch_list_name
            report_df_combined.append(report_df)
    if not sep_files:
        res_df_final = pd.DataFrame(pd.concat(res_df_combined,axis=0,ignore_index=True))
        report_df_final = pd.DataFrame(pd.concat(report_df_combined,axis=0,ignore_index=True))
        out_loc = out_loc + '.xls'
        save_res_to_file_auto_format(res_df_final, report_df_final, out_loc)
    return

if __name__ == "__main__":
    optparser = OptionParser()
    optparser.add_option('-n', '--name',
                         dest='list_name',
                         help='name of the list',
                         default=None)
    optparser.add_option('-d', '--designations',
                         dest='desig_loc',
                         help='location of csv containing target designations',
                         default=None)
    optparser.add_option('-o', '--out_file',
                         dest='out_file',
                         help='location of csv containing people details ',
                         default='people_details_extracted.csv')
    optparser.add_option('-b', '--batch_save',
                         dest='batch_save',
                         help='do batch save if 1',
                         default=0,type='int')
    optparser.add_option('-s', '--sep_files',
                         dest='sep_files',
                         help='if batch save, results will be saved in separate files if 1',
                         default=1,type='int')
    (options, args) = optparser.parse_args()
    list_name = options.list_name
    out_file = options.out_file
    desig_loc = options.desig_loc
    batch_save = options.batch_save
    sep_files = options.sep_files
    if not batch_save:
        save_to_file(list_name,desig_loc,out_file)
    else:
        save_to_file_batch(list_name,desig_loc,out_file,sep_files)