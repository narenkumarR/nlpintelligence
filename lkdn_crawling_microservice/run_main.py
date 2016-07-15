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
    (options, args) = optparser.parse_args()
    csv_company = options.csv_company
    desig_loc = options.desig_loc
    list_name = options.list_name
    similar_companies = options.similar_companies
    hours = options.no_hours
    extract_urls = options.extract_urls

    while True:
        # try:
            os.system('pkill -9 firefox')
            os.system('pkill -9 Xvfb')
            os.system("find /tmp/* -maxdepth 1 -type d -name 'tmp*' |  xargs rm -rf")
            os.system('python main.py -n {} -f {} -d {} -s {} -t {} -u {}'.format(list_name,csv_company,desig_loc,similar_companies,hours,extract_urls))
            time.sleep(10)
        # except:
        #     continue
