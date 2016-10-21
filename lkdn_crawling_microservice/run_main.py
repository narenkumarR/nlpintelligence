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
    optparser.add_option('-m', '--main_thread',
                         dest='main_thread',
                         help='if 1, main thread, table updation should happen, else only crawling',
                         default=0,type='float')
    optparser.add_option('-r', '--nthread',
                         dest='n_threads',
                         help='if 1, main thread, table updation should happen, else only crawling',
                         default=2,type='int')
    optparser.add_option('-k', '--nurls',
                         dest='n_urls',
                         help='no of urls in a thread',
                         default=100,type='int')
    optparser.add_option('-i', '--iters',
                         dest='n_iters',
                         help='no of iterations. default 0. if 0,program run is based on time',
                         default=0,type='int')
    optparser.add_option('-l', '--login',
                         dest='login',
                         help='login using credentials given in constants file',
                         default=0,type='int')

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
    n_threads = int(options.n_threads)
    n_urls = options.n_urls
    n_iters = options.n_iters
    if n_iters:
        n_iter = 1
    else:
        n_iter = 0
    login = options.login
    while True:
        try:
            # os.system('pkill -9 firefox')
            # os.system('pkill -9 Xvfb')
            # os.system("find /tmp/* -maxdepth 1 -type d -name 'tmp*' |  xargs rm -rf")
            # os.system("find /tmp/* -maxdepth 0 -type f -name 'tmpaddon*' | xargs rm -rf")
            os.system('python main.py -n {} -f {} -d {} -s {} -t {} -u {} -p {} -q "{}" -v {} -w {} -m {} -r {} -k {} -i {} -l {}'.format(list_name,csv_company,
                                                                                        desig_loc,similar_companies,
                                                                                        hours,extract_urls,prospect_db,prospect_query,
                                                                                        visible,what,main_thread,
                                                                                        n_threads,n_urls,n_iter,login))
            time.sleep(10)
            # after the first iteration, no need to look for prospect data # not correct. only if not looking for
            # similar companies, this is needed. so adding similar company condition also here
            if prospect_db and not similar_companies:
                prospect_db = 0
            if n_iters:
                n_iters = n_iters -1
                if n_iters <= 0:
                    break
        except:
            # os.system('pkill -9 firefox')
            # os.system('pkill -9 Xvfb')
            # os.system("find /tmp/* -maxdepth 1 -type d -name 'tmp*' |  xargs rm -rf")
            # os.system("find /tmp/* -maxdepth 0 -type f -name 'tmpaddon*' | xargs rm -rf")
            break
