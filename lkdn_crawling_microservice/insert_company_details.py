__author__ = 'joswin'

import pandas as pd
import logging

from optparse import OptionParser

from postgres_connect import PostgresConnect
from crawling_micro_service.crawler_generic import LinkedinCrawlerThread
from crawling_micro_service.tables_updation import TableUpdater

from constants import company_name_field,company_details_field,designations_column_name

def run_main(list_name=None,company_csv_loc=None,desig_loc=None,similar_companies=1,hours=1,extract_urls=1):
    '''
    :param list_name:
    :param company_csv_loc:
    :param desig_loc:
    :return:
    '''
    # logging.basicConfig(filename='log_file.log', level=logging.INFO,format='%(asctime)s %(message)s')
    logging.info('started main program')
    if not list_name:
        raise ValueError('list name must be provided')
    if company_csv_loc and company_csv_loc != 'None':
        inp_df = pd.read_csv(company_csv_loc)
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
        raise Exception('list name not present in the tables')
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
    (options, args) = optparser.parse_args()
    csv_company = options.csv_company
    list_name = options.list_name

    run_main(list_name,csv_company)

