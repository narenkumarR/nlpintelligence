__author__ = 'joswin'
import pandas as pd
import logging
from optparse import OptionParser
import crawling_micro_service.sheduler_login
from postgres_connect import PostgresConnect
from constants import designations_column_name

def run_main_login(list_name=None,desig_loc=None,visible=False,no_pages_to_search=2,final_run=0,people_crawl=0):
    '''
    :param list_name:
    :param desig_loc:
    :param visible:
    :param no_pages_to_search:
    :param final_run:
    :param people_crawl:
    :return:
    '''
    list_table = 'crawler.list_table'
    logging.basicConfig(filename='log_file_login.log', level=logging.INFO,format='%(asctime)s %(message)s')
    logging.info('started main program ')
    con = PostgresConnect()
    con.get_cursor()
    if not list_name:
        raise ValueError('list name must be provided')
    if desig_loc and desig_loc != 'None':
        inp_df = pd.read_csv(desig_loc)
        desig_list = list(inp_df[designations_column_name])
    else:
        desig_list = None
    query = 'select id from {} where list_name = %s'.format(list_table)
    con.cursor.execute(query,(list_name,))
    res_list = con.cursor.fetchall()
    if len(res_list) == 0 :
        raise ValueError('list name provided does not exist')
    list_id = res_list[0][0]
    cc = crawling_micro_service.sheduler_login.LinkedinLoginCrawlerThread(visible=visible)
    cc.run(list_id = list_id,company_base_table='crawler.linkedin_company_base_login',desig_list=desig_list,
           no_pages_to_search=no_pages_to_search,final_run=final_run,people_crawl=people_crawl)
    logging.info('completed main program ')

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
    optparser.add_option('-v', '--visible',
                         dest='visible',
                         help='visible if 1',
                         default=0,type='float')
    optparser.add_option('-p', '--no_pages',
                         dest='no_pages',
                         help='no of pages to search for people (default 2)',
                         default=2,type='int')
    optparser.add_option('-f', '--final_run',
                         dest='final_run',
                         help='if final run, take all companies for whom people not availabe in the people email table '
                              'and run the crawling for all',
                         default=0,type='int')
    optparser.add_option('-P', '--people_crawl',
                         dest='people_crawl',
                         help='crawl people pages ',
                         default=0,type='int')
    (options, args) = optparser.parse_args()
    list_name = options.list_name
    desig_loc = options.desig_loc
    visible = options.visible
    no_pages = options.no_pages
    final_run = options.final_run
    people_crawl = options.people_crawl
    run_main_login(list_name,desig_loc,visible,no_pages,final_run,people_crawl)