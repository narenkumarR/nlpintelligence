__author__ = 'joswin'

import os
import time
from optparse import OptionParser

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
    optparser.add_option('-p', '--prospDB',
                         dest='prospect_db',
                         help='Fetch data from prospect db', #if not, directly go to crawling
                         default=0,type='float')
    optparser.add_option('-v', '--visible',
                         dest='visible',
                         help='visible if 1',
                         default=0,type='float')
    optparser.add_option('-w', '--what',
                         dest='what',
                         help='if 1, only companies, if 2, only people, if 0, both',
                         default=0,type='float')
    optparser.add_option('-m', '--main_thread',
                         dest='main_thread',
                         help='if 1, main thread, table updation should happen, else only crawling',
                         default=0,type='float')
    optparser.add_option('-r', '--nthread',
                         dest='n_threads',
                         help='if 1, main thread, table updation should happen, else only crawling',
                         default=2,type='int')

    (options, args) = optparser.parse_args()
    csv_company = options.csv_company
    desig_loc = options.desig_loc
    list_name = options.list_name
    similar_companies = options.similar_companies
    hours = options.no_hours
    extract_urls = options.extract_urls
    prospect_db = options.prospect_db
    visible = options.visible
    what = options.what
    main_thread = options.main_thread
    n_threads = options.n_threads

    while True:
        # try:
            os.system('pkill -9 firefox')
            os.system('pkill -9 Xvfb')
            os.system("find /tmp/* -maxdepth 1 -type d -name 'tmp*' |  xargs rm -rf")
            os.system('python main.py -n {} -f {} -d {} -s {} -t {} -u {} -p {} -v {} -w {} -m {} -r {}'.format(list_name,csv_company,
                                                                                        desig_loc,similar_companies,
                                                                                        hours,extract_urls,prospect_db,
                                                                                        visible,what,main_thread,
                                                                                        n_threads))
            time.sleep(10)
        # except:
        #     continue
