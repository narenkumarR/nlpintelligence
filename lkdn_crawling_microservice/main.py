__author__ = 'joswin'

import gc
import pandas as pd
import time
import logging
import os
# import threading
import multiprocessing

from optparse import OptionParser

from postgres_connect import PostgresConnect
from linkedin_url_finder import LkdnUrlExtrMain
from crawling_micro_service.sheduler_combined import LinkedinCrawlerThread
from tables_updation import TableUpdater
from fetch_prospectsdb_data import FetchProspectDB
# from gen_people_for_email import gen_people_details

from constants import company_name_field,company_details_field,designations_column_name

def run_main(list_name=None,company_csv_loc=None,desig_loc=None,similar_companies=1,hours=1,extract_urls=1,
             prospect_db = 0, prospect_query = '',visible=False,what=0,main_thread=0,n_threads=2,n_urls_in_thread=0,
             n_iters=0,login=0,browser_name='Firefox'):
    '''
    :param list_name:
    :param company_csv_loc:
    :param desig_loc:
    :param similar_companies:
    :param hours:
    :param extract_urls:
    :param prospect_db:if 1, fetch from prospect db(only if main thread also is 1,otherwise ignored)
    :param prospect_query:
    :param visible:
    :param what: if 1 crawl companies,2 : people, 0: both
    :param main_thread: if 1, table updation will be run else only crawling
    :return:
    '''
    logging.basicConfig(filename='log_file_{}.log'.format(list_name), level=logging.INFO,format='%(asctime)s %(message)s')
    logging.info('started main program. n_iter:{}'.format(n_iters))
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
        if not company_csv_loc and not prospect_db:
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
    # os.system("find /tmp/* -maxdepth 1 -type d -name 'tmp*' |  xargs rm -rf")
    if main_thread:
        logging.info('updating tables for iteration')
        try:
            tables_updater.update_tables(list_id,desig_list,similar_companies,company_select_query=prospect_query)
        except:
            logging.exception('Error while updating tables. Continue without updation')
    if prospect_db :
        #this is done before urls extraction because some domains will be already present in
        #prospect db even though linkedin_url not present in the list.
        logging.info('Fetching data in Prospect Database')
        fp = FetchProspectDB()
        try:
            fp.fetch_data(list_id,prospect_query,desig_list=desig_list)
        except:
            logging.exception('Error while fetching data. Continue without fetching.')
        logging.info('Completed fetching data from prospect db')
    if extract_urls: #better to run this process separately
        url_extractor = LkdnUrlExtrMain(visible=visible)
        logging.info('find linkedin urls')
        # os.system("pkill -9 firefox")
        t1 = multiprocessing.Process(target=url_extractor.run_main, args=(list_id,))
        t1.daemon = True
        t1.start()
        time.sleep(120)
        # url_extractor.run_main(list_id,threads=1)
        if prospect_db :
            #after finding linkedin_urls, look in prospect db again
            logging.info('Fetching data in Prospect Database after linkedin url extraction')
            fp = FetchProspectDB()
            try:
                fp.fetch_data(list_id,prospect_query,desig_list=desig_list)
            except:
                logging.exception('Error while fetching data. Continue without fetching.')
            logging.info('Completed fetching data from prospect db (after linkedin url extraction)')
    gc.collect()
    start_time = time.time()
    limit_no_1 = n_threads*n_urls_in_thread
    what_bck = what
    if login:
        n_threads = 1 #only single thread if login
        if not what_bck:
            what = 1 #either company or people will be crawled in 1 iteration if login.
    while True:
        if not n_iters:
            if time.time() - start_time > hours*60*60:
                break
        logging.info('starting an iteration of crawling')
        crawler.run_both_single(list_id=list_id,visible=visible,limit_no=limit_no_1,time_out = hours,what=what,
                                n_threads=n_threads,login=login,browser=browser_name)
        if main_thread:
            # if prospect_db:
            #     logging.info('Fetching data in Prospect Database')
            #     fp.fetch_data(list_id,prospect_query,desig_list=desig_list)
            #     logging.info('Completed fetching data from prospect db')
            logging.info('updating tables for iteration')
            try:
                tables_updater.update_tables(list_id,desig_list,similar_companies,company_select_query=prospect_query)
            except:
                logging.exception('Error while updating tables. Continue without updation')
        #os.system("pkill -9 firefox")
        #os.system("pkill -9 Xvfb")
        #os.system("find /tmp/* -maxdepth 1 -type d -name 'tmp*' |  xargs rm -rf")
        # if main_thread:
        #     gen_people_details(list_id,desig_list)
        if n_iters:
            n_iters = n_iters - 1
            if n_iters <= 0 :
                break
        if login:
            if not what_bck:
                what = 3-what #toggle what if crawling both people and company

    del crawler,tables_updater
    if extract_urls:
        if t1.is_alive():
            t1.terminate()
        del url_extractor
    # os.system("pkill -9 firefox")
    # os.system("find /tmp/* -maxdepth 1 -type d -name 'tmp*' |  xargs rm -rf")
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
                         help='No of hours the process need to run. Default 1 hour. Used only when --iters is 0',
                         default=1,type='float')
    optparser.add_option('-u', '--urlextr',
                         dest='extract_urls',
                         help='Extract urls', #if not, directly go to crawling
                         default=1,type='float')
    optparser.add_option('-p', '--prospDB',
                         dest='prospect_db',
                         help='Fetch data from prospect db', #if not, directly go to crawling
                         default=0,type='float')
    optparser.add_option('-q', '--prospQuery',
                         dest='prospect_query',
                         help='Query for fetching data from prospect db', #if not, directly go to crawling
                         default='')
    optparser.add_option('-v', '--visible',
                         dest='visible',
                         help='visible if 1',
                         default=0,type='float')
    optparser.add_option('-w', '--what',
                         dest='what',
                         help='if 1, only companies, if 2, only people, if 0, both',
                         default=0,type='float')
    optparser.add_option('-m', '--mainthread',
                         dest='main_thread',
                         help='if 1, main thread, table updation should happen, else only crawling',
                         default=0,type='float')
    optparser.add_option('-r', '--nthread',
                         dest='n_threads',
                         help='no of threads to be run in parallel',
                         default=2,type='int')
    optparser.add_option('-k', '--nurls',
                         dest='n_urls',
                         help='no of urls in a thread',
                         default=0,type='int')
    optparser.add_option('-i', '--iters',
                         dest='n_iters',
                         help='no of iterations. default 0. if 0,program run is based on time',
                         default=0,type='int')
    optparser.add_option('-l', '--login',
                         dest='login',
                         help='login using credentials given in constants file',
                         default=0,type='int')
    optparser.add_option('-b', '--browser',
                         dest='browser',
                         help='browser name (Firefox,Firefox_luminati)',
                         default='Firefox')
    (options, args) = optparser.parse_args()
    csv_company = options.csv_company
    desig_loc = options.desig_loc
    list_name = options.list_name
    similar_companies = options.similar_companies
    hours = options.no_hours
    extract_urls = options.extract_urls
    prospect_db = options.prospect_db
    prospect_query = options.prospect_query
    visible = options.visible
    what = options.what
    main_thread = options.main_thread
    n_threads = options.n_threads
    n_urls = options.n_urls
    n_iters = options.n_iters
    login = options.login
    browser = options.browser
    run_main(list_name,csv_company,desig_loc,similar_companies,hours,extract_urls,prospect_db,prospect_query,
             visible=visible,what=what,main_thread = main_thread,n_threads=n_threads,n_urls_in_thread=n_urls,
             n_iters = n_iters,login=login,browser_name=browser)


