__author__ = 'joswin'
# -*- coding: utf-8 -*-
import datetime
import logging
import pandas as pd
from optparse import OptionParser
from random import shuffle
import multiprocessing


from constants import website_column
from postgres_connect import PostgresConnect


from webpage_crawler import WebCrawlerScheduler

logging.basicConfig(filename='logs/website_extraction_{}.log'.format(datetime.datetime.now()),
                    level=logging.INFO,format='%(asctime)s %(message)s')

def chunkify(lst,n):
    return [ lst[i::n] for i in xrange(n) ]

class WebCrawlerParallel(object):
    def __init__(self):
        pass

    def run_crawl_parallel_table_input(self,table_name,n_process=3,visible=False,min_pages=3):
        logging.info('run_website_crawl_table_input started for table:{}'.format(table_name))
        con = PostgresConnect()
        con.connect()
        con.cursor.execute("select distinct domain from {} where extraction_tried='f' ".format(table_name))
        websites = con.cursor.fetchall()
        con.close_connection()
        websites = [i[0] for i in websites]
        shuffle(websites)
        logging.info('no of websites for which crawling will be done:{}'.format(len(websites)))
        jobs = []
        for websites_chunk in chunkify(websites,n_process):
            wcs = WebCrawlerScheduler()
            p = multiprocessing.Process(target=wcs.run_website_crawl,args=(websites_chunk,table_name,visible,min_pages))
            p.daemon = True
            jobs.append(p)
            p.start()
        for p in jobs:
            p.join()
        logging.info('completed run_website_crawl_table_input')

    def run_crawl_parallel_csv_input(self,website_file,table_name,n_process=3, visible=False,min_pages=3):
        df = pd.read_csv(website_file)
        domains = df[website_column]
        con = PostgresConnect()
        con.connect()
        con.cursor.execute("drop table if exists {}".format(table_name))
        con.cursor.execute("create table {} (domain text,extraction_tried boolean default false,"
                           "extraction_success boolean default false)".format(table_name))
        domains = [(i,) for i in list(domains)]
        records_list_template = ','.join(['%s']*len(domains))
        insert_query = "INSERT INTO {} (domain) VALUES {} ".format(table_name,records_list_template)
        con.cursor.execute(insert_query, domains)
        con.commit()
        con.close_connection()
        self.run_crawl_parallel_table_input(table_name,n_process,visible,min_pages)

if __name__ == "__main__":
    optparser = OptionParser()
    optparser.add_option('-w', '--websites',
                         dest='website_file',
                         help='location of websites csv file',
                         default=None)
    optparser.add_option('-t', '--table_name',
                         dest='table_name',
                         help='table containing websites to crawl',
                         default=None)
    optparser.add_option('-v', '--visible',
                         dest='visible',
                         help='if 1 visible, if 0 not visible',
                         default=0,type='int')
    optparser.add_option('-m', '--minpages',
                         dest='minpages',
                         help='no of pages to crawl within a url',
                         default=3,type='int')
    optparser.add_option('-n', '--n_process',
                         dest='n_process',
                         help='n_process',
                         default=3,type='int')

    (options, args) = optparser.parse_args()
    website_file = options.website_file
    table_name = options.table_name
    visible = options.visible
    min_pages = options.minpages
    n_process = options.n_process
    wcp = WebCrawlerParallel()
    logging.info('started crawling')
    if website_file:
        wcp.run_crawl_parallel_csv_input(website_file,table_name,n_process,visible=visible,min_pages=min_pages)
    elif table_name:
        wcp.run_crawl_parallel_table_input(table_name,n_process,visible,min_pages)
    logging.info('completed crawling')