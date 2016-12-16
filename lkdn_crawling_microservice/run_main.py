__author__ = 'joswin'

import os
import time
from optparse import OptionParser

if __name__ == "__main__":
    optparser = OptionParser()
    optparser.add_option('-n', '--name',
                         dest='list_name',
                         help='name of the list',
                         default=None,type='str')
    optparser.add_option('-f', '--csvfile',
                         dest='csv_company',
                         help='location of csv with company names',
                         default=None)
    optparser.add_option('-d', '--designations',
                         dest='desig_loc',
                         help='location of csv containing target designations',
                         default=None)
    optparser.add_option('-D', '--designations_login',
                         dest='desig_loc_login',
                         help='location of csv containing target designations for login crawling. '
                              'this should be list of designations need to be searched in linkedin',
                         default=None)
    optparser.add_option('-s', '--similar',
                         dest='similar_companies',
                         help='give 1 if similar companies need to be found.Else 0',
                         default=0,type='float')
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
                         default=1,type='float')
    optparser.add_option('-r', '--nthread',
                         dest='n_threads',
                         help='no of threads',
                         default=4,type='int')
    optparser.add_option('-k', '--nurls',
                         dest='n_urls',
                         help='no of urls in a thread',
                         default=100,type='int')
    optparser.add_option('-i', '--iters',
                         dest='n_iters',
                         help='no of iterations. default 0. if 0,program run is based on time',
                         default=4,type='int')
    optparser.add_option('-l', '--login',
                         dest='login',
                         help='login using credentials given in constants file',
                         default=1,type='int')
    optparser.add_option('-c', '--crawl_normal',
                         dest='crawl_normal',
                         help='do crawling without logging',
                         default=1,type='int')
    optparser.add_option('-o', '--out_file',
                         dest='out_file',
                         help='location of csv containing people details ',
                         default=False)
    optparser.add_option('-b', '--browser',
                         dest='browser',
                         help='browser name (Firefox,Firefox_luminati)',
                         default='Firefox')
    (options, args) = optparser.parse_args()
    csv_company = options.csv_company
    desig_loc = options.desig_loc
    desig_loc_login = options.desig_loc_login
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
    login = options.login
    crawl_normal = options.crawl_normal
    out_file = options.out_file
    browser = options.browser
    if n_iters:
        n_iter = 1
    else:
        n_iter = 0
    if csv_company:
        print('list_name:{} inserting details into table'.format(list_name))
        os.system('python company_linkedin_urls_manual_insert.py -n {list_name} -f {csv_company}'.format(
            list_name=list_name,csv_company=csv_company
        ))
    while True:
        print('list_name:{} running an iteration of crawling: iter remaining:{}'.format(list_name,n_iters))
        if not crawl_normal:
            break
        try:
            # os.system('pkill -9 firefox')
            # os.system('pkill -9 Xvfb')
            # os.system("find /tmp/* -maxdepth 0 -type d -name 'tmp*' |  xargs rm -rf")
            # os.system("find /tmp/* -maxdepth 0 -type f -name 'tmpaddon*' | xargs rm -rf")
            os.system('python main.py -n {list_name} -f {csv_company} -d {desig_loc} -s {similar_companies} '
                      '-t {hours} -u {extract_urls} -p {prospect_db} -q "{prospect_query}" -v {visible} -w {what}'
                      ' -m {main_thread} -r {n_threads} -k {n_urls} -i {n_iters} -l {login} -b {browser}'.format(
                list_name=list_name,csv_company=None,desig_loc=desig_loc,similar_companies=similar_companies,
                hours=hours,extract_urls=extract_urls,prospect_db=prospect_db,prospect_query=prospect_query,
                visible=visible,what=what,main_thread=main_thread,n_threads=n_threads,n_urls=n_urls,n_iters=n_iter,
                login=0,browser=browser
            ))
            # extract_urls = False
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
    # gen people details
    print('list_name:{} generating people details'.format(list_name))
    os.system("python gen_people_for_email.py -n {list_name} -d {desig_loc} ".format(
        list_name=list_name,desig_loc=desig_loc
    ))
    if login:
        print('list_name:{} generating people details using login table'.format(list_name))
        os.system("python gen_people_for_email.py -n {list_name} -d {desig_loc} -C {comp_login_table}".format(
            list_name=list_name,desig_loc=desig_loc,comp_login_table='crawler.linkedin_company_base_login'
        ))
        print('list_name:{} running the crawler with loggin in'.format(list_name))
        os.system("python main_login.py -n {list_name} -d {desig_loc_login} -v {visible} -f 1 -P 1".format(
            list_name=list_name,desig_loc_login=desig_loc_login,visible=visible
        ))
        print('list_name:{} generating people details after logging in'.format(list_name))
        os.system("python gen_people_for_email.py -n {list_name} -d {desig_loc} -C {comp_login_table}".format(
            list_name=list_name,desig_loc=desig_loc,comp_login_table='crawler.linkedin_company_base_login'
        ))
    # save results to file
    if out_file:
        print('list_name:{} saving results into file'.format(list_name))
        os.system("python save_output_to_file.py -n {list_name} -o {out_file}".format(list_name=list_name,out_file=out_file))
