__author__ = 'joswin'

import gc
import pandas as pd
import time
import logging
import os
import threading
import multiprocessing

from optparse import OptionParser

from postgres_connect import PostgresConnect
from linkedin_company_url_extraction_micro_service.linkedin_url_finder import LkdnUrlExtrMain
from crawling_micro_service.crawler_generic import LinkedinCrawlerThread
from crawling_micro_service.tables_updation import TableUpdater
from gen_people_for_email import gen_people_details

from constants import company_name_field,company_details_field,designations_column_name

def run_main(list_name=None,company_csv_loc=None,desig_loc=None,similar_companies=1,hours=1,extract_urls=1,visible=False):
    '''
    :param list_name:
    :param company_csv_loc:
    :param desig_loc:
    :return:
    '''
    logging.basicConfig(filename='log_file.log', level=logging.INFO,format='%(asctime)s %(message)s')
    logging.info('started main program')
    if not list_name:
        raise ValueError('list name must be provided')
    if company_csv_loc and company_csv_loc != 'None':
        inp_df = pd.read_csv(company_csv_loc)
        inp_df = inp_df.fillna('')
        inp_list = [(inp_df.iloc[i][company_name_field],inp_df.iloc[i][company_details_field]) for i in range(inp_df.shape[0])]
    else:
        inp_list = []
    if desig_loc and desig_loc != 'None':
        inp_df = pd.read_csv(desig_loc)
        desig_list = list(inp_df[designations_column_name])
    else:
        desig_list = None
    list_table = 'crawler.list_table'
    list_items_table = 'crawler.list_items'
    con = PostgresConnect()
    crawler = LinkedinCrawlerThread()
    tables_updater = TableUpdater()
    query = 'select id from {} where list_name = %s'.format(list_table)
    con.get_cursor()
    con.cursor.execute(query,(list_name,))
    res_list = con.cursor.fetchall()
    if len(res_list) == 0 :
        if not company_csv_loc:
            raise Exception('No data with the list name present in database. Need to provide csv file with data')
        query = 'insert into {} (list_name) values (%s)'.format(list_table)
        con.cursor.execute(query,(list_name,))
        query = 'select id from {} where list_name = %s'.format(list_table)
        con.cursor.execute(query,(list_name,))
        res_list = con.cursor.fetchall()
        list_id = res_list[0][0]
    else:
        list_id = res_list[0][0]
    if inp_list:
        records_list_template = ','.join(['%s']*len(inp_list))
        insert_query = "INSERT INTO {} (list_id,list_input,list_input_additional) VALUES {} "\
         "ON CONFLICT DO NOTHING".format(list_items_table,records_list_template)
        inp_list1 = [(list_id,i[0],i[1],) for i in inp_list]
        con.cursor.execute(insert_query, inp_list1)
    con.commit()
    con.close_cursor()
    os.system("find /tmp/* -maxdepth 1 -type d -name 'tmp*' |  xargs rm -rf")
    if extract_urls:
        url_extractor = LkdnUrlExtrMain(visible=visible)
        logging.info('going to find linkedin urls')
        # os.system("pkill -9 firefox")
        t1 = multiprocessing.Process(target=url_extractor.run_main, args=(list_id,))
        t1.daemon = True
        t1.start()
        time.sleep(120)
    gc.collect()
    start_time = time.time()
    # if similar_companies is 0, remove all in urls to crawl table
    while True:
        if time.time() - start_time > hours*60*60:
            break
        # os.system("find /tmp/* -maxdepth 1 -type d -name 'tmp*' |  xargs rm -rf")
        # os.system("pkill -9 firefox")
        logging.info('starting an iteration of crawling')
        logging.info('updating tables for iteration')
        tables_updater.update_tables(list_id,desig_list,similar_companies)
        crawler.run_both_single(list_id=list_id,visible=visible,limit_no=100,time_out = hours)
        # os.system("find /tmp/* -maxdepth 1 -type d -name 'tmp*' |  xargs rm -rf")
        # os.system("pkill -9 firefox")
        gen_people_details(list_id,desig_list)
    del crawler,tables_updater
    if extract_urls:
        if t1.is_alive():
            t1.terminate()
        del url_extractor
    # os.system("pkill -9 firefox")
    os.system("find /tmp/* -maxdepth 1 -type d -name 'tmp*' |  xargs rm -rf")
    gc.collect()
    logging.info('completed the main program')

if __name__ == "__main__":
    optparser = OptionParser()
    optparser.add_option('-n', '--name',
                         dest='list_name',
                         help='name of the list',
                         default=None)
    optparser.add_option('-f', '--csvfile',
                         dest='csv_company',
                         help='location of csv with company names',
                         default=None)
    optparser.add_option('-d', '--designations',
                         dest='desig_loc',
                         help='location of csv containing target designations',
                         default=None)
    optparser.add_option('-s', '--similar',
                         dest='similar_companies',
                         help='give 1 if similar companies need to be found.Else 0',
                         default=1,type='float')
    optparser.add_option('-t', '--hours',
                         dest='no_hours',
                         help='No of hours the process need to run. Default 1 hour',
                         default=1,type='float')
    optparser.add_option('-u', '--urlextr',
                         dest='extract_urls',
                         help='Extract urls', #if not, directly go to crawling
                         default=1,type='float')
    (options, args) = optparser.parse_args()
    csv_company = options.csv_company
    desig_loc = options.desig_loc
    list_name = options.list_name
    similar_companies = options.similar_companies
    hours = options.no_hours
    extract_urls = options.extract_urls

    run_main(list_name,csv_company,desig_loc,similar_companies,hours,extract_urls)


