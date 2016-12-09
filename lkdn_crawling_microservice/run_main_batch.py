__author__ = 'joswin'
import os
import signal
import time
from optparse import OptionParser
from subprocess import Popen

from postgres_connect import PostgresConnect
from company_linkedin_urls_manual_insert import upload_url_list_batch
from save_output_to_file import save_to_file_batch

# args to be used by command_generator
run_main_mapping = {
    'desig_loc':'-d','desig_loc_login':'-D','extract_urls':'-u','prospect_db':'-p',
    'visible':'-v','what':'-w','login':'-l','crawl_normal':'-c','browser':'-b',
    'main_thread':'-m','n_threads':'-r','n_urls_in_thread':'-k','n_iters':'-i','hours':'-t'
}

def _launch_new_process( argument_generator):
    '''launch command from argument generator in popen
    :param base_command: command to run
    :param argument_generator: extra arguments
    :return:
    '''
    process_query_list =  next(argument_generator)
    print "Launching {}".format(process_query_list)
    child_process = Popen(process_query_list)
    return child_process


def command_generator(list_name,low_ind=0,high_ind=-1,**kwargs):
    '''commands which can be given are given below
    :param list_name:
    :param desig_loc:
    :param desig_loc_login:
    :param extract_urls:
    :param prospect_db:
    :param visible:
    :param what:
    :param login:
    :param crawl_normal:
    :param browser:
    :param low_ind:
    :param high_ind:
    :param main_thread:
    :param n_threads:
    :param n_urls_in_thread:
    :param n_iters:
    :param hours:
    :return:
    '''
    print('generating command for list_name: {}'.format(list_name))
    con = PostgresConnect()
    con.get_cursor()
    list_name_like = list_name+'%'
    query = "select list_name from crawler.list_table where list_name like %s order by list_name"
    con.cursor.execute(query,(list_name_like,))
    res_list = con.cursor.fetchall()
    con.close_cursor()
    con.close_connection()
    if high_ind == -1:
        res_list = res_list[low_ind:]
    else:
        res_list = res_list[low_ind:high_ind]
    print('the list names to be processed:{}'.format(res_list))
    args_list = ['-s','0','-q','','-o','']
    for key, value in kwargs.iteritems():
        if key in run_main_mapping :
            args_list.append(run_main_mapping[key])
            args_list.append(str(value))
    for tup1 in res_list:
        batch_list_name = tup1[0]
        # sample command expected from here as a list:
        # "python run_main.py -n {list_name} -d {desig_loc} -D {desig_loc_login} -u {extract_url} " \
        #           "-p {prospect_db} -v {visible} -w {what_to_crawl} -l {login} -c {normal_crawl} -b {browser} " \
        #           "-m {main_thread} -r {n_threads} -k {urls_in_thread} -i {n_iters} -h {hours} -s {similar_comps} " \
        #           "-q {prospect_query} -o {out_loc}".format(list_name=batch_list_name,desig_loc=desig_loc,
        #                                        desig_loc_login=desig_loc_login,extract_url=extract_urls,
        #                                        prospect_db=prospect_db,visible=visible,what_to_crawl=what,login=login,
        #                                        normal_crawl=crawl_normal,browser=browser,main_thread=main_thread,
        #                                        n_threads=n_threads,urls_in_thread=n_urls,n_iters=n_iters,hours=hours,
        #                                        similar_comps=0,prospect_query=0,out_loc=None)
        command_list = ["python","run_main.py","-n",batch_list_name]+args_list
        yield command_list


def launch_fixed_processes(process_count,  argument_generator):
    ''' launch processes
    :param process_count:
    :param argument_generator:
    :return:
    '''
    print('Started crawling process for list name: {}'.format(list_name))
    active_child_processes = []
    # args = base_command.split()
    # import pdb
    # pdb.set_trace()
    try: #this try for catching keyboard interrupts
        while True:
            # print active_child_processes
            active_child_processes = _filter_processes_still_active(active_child_processes)
            if len(active_child_processes) < process_count:
                try:
                    child_process = _launch_new_process( argument_generator)
                except StopIteration:
                    break
                except:
                    continue
                active_child_processes.append(child_process)
        while len(active_child_processes) > 0:
            active_child_processes = _filter_processes_still_active(active_child_processes)
            time.sleep(120)
    except KeyboardInterrupt:
        active_child_processes = _filter_processes_still_active(active_child_processes)
        for proc in active_child_processes:
            if proc.pid is None:
                pass
            else:
                os.kill(proc.pid, signal.SIGTERM)
        raise ValueError('\n\n\n**********************************************************************************\n'
                         '**********************************************************************************\n'
                         ' Process terminated by keyboard interrupt.\nNeed to clean up tmp folders and kill '
                         ' browsers manually\n'
                         '**********************************************************************************\n'
                         '**********************************************************************************\n\n\n'
        )

def _filter_processes_still_active(processes):
    ''' get all the processes still alive
    :param processes:
    :return:
    '''
    return [process for process in processes if process.poll() is None]


if __name__ == "__main__":
    optparser = OptionParser()
    optparser.add_option('-n', '--name',
                         dest='list_name',
                         help='name of the list',
                         default='',type='str')
    optparser.add_option('-f', '--csvfile',
                         dest='csv_company',
                         help='location of csv with company names',
                         default='')
    optparser.add_option('-d', '--designations',
                         dest='desig_loc',
                         help='location of csv containing target designations',
                         default='')
    optparser.add_option('-D', '--designations_login',
                         dest='desig_loc_login',
                         help='location of csv containing target designations for login crawling. '
                              'this should be list of designations need to be searched in linkedin',
                         default='')
    optparser.add_option('-u', '--urlextr',
                         dest='extract_urls',
                         help='Extract urls', #if not, directly go to crawling
                         default=1,type='float')
    optparser.add_option('-p', '--prospDB',
                         dest='prospect_db',
                         help='Fetch data from prospect db', #if not, directly go to crawling
                         default=1,type='float')
    optparser.add_option('-m', '--main_thread',
                         dest='main_thread',
                         help='if 1, main thread, table updation should happen, else only crawling',
                         default=1,type='float')
    optparser.add_option('-v', '--visible',
                         dest='visible',
                         help='visible if 1',
                         default=0,type='float')
    optparser.add_option('-w', '--what',
                         dest='what',
                         help='if 1, only companies, if 2, only people, if 0, both',
                         default=0,type='float')
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
    optparser.add_option('-S', '--sep_files',
                         dest='sep_files',
                         help='if 1, output from each batch will be saved in a separate file',
                         default=False)
    optparser.add_option('-b', '--browser',
                         dest='browser',
                         help='browser name (Firefox,Firefox_luminati)',
                         default='Firefox')
    optparser.add_option('-L', '--low_ind',
                         dest='low_ind',
                         help='lower index of the batches',
                         default=0,type='int')
    optparser.add_option('-H', '--high_ind',
                         dest='high_ind',
                         help='higher index of the batches',
                         default=-1,type='int')
    optparser.add_option('-B', '--n_batches',
                         dest='n_batches',
                         help='no of batches to run simultaneously',
                         default=2,type='int')
    optparser.add_option('-r', '--nthread',
                         dest='n_threads',
                         help='no of threads',
                         default=3,type='int')
    optparser.add_option('-k', '--nurls_thread',
                         dest='n_urls_thread',
                         help='no of urls in a thread',
                         default=333,type='int')
    optparser.add_option('-i', '--iters',
                         dest='n_iters',
                         help='no of iterations. default 0. if 0,program run is based on time',
                         default=7,type='int')
    optparser.add_option('-t', '--hours',
                         dest='no_hours',
                         help='No of hours the process need to run. Default 1 hour',
                         default=3,type='float')

    (options, args) = optparser.parse_args()
    csv_company = options.csv_company
    desig_loc = options.desig_loc
    desig_loc_login = options.desig_loc_login
    list_name = options.list_name
    extract_urls = options.extract_urls
    prospect_db = options.prospect_db
    main_thread = options.main_thread
    visible = options.visible
    what = options.what
    login = options.login
    crawl_normal = options.crawl_normal
    out_file = options.out_file
    sep_files = options.sep_files
    browser = options.browser
    low_ind = options.low_ind
    high_ind = options.high_ind
    n_batches = options.n_batches
    n_threads = options.n_threads
    n_urls_thread = options.n_urls_thread
    n_iters = options.n_iters
    hours = options.no_hours

    print('started batch processing, list name:{}'.format(list_name))
    if csv_company:
        print('inserting from file to table')
        upload_url_list_batch(csv_company,list_name,no_urls_in_batch=500)
    print('starting crawler subprocesses')
    launch_fixed_processes(n_batches,
           command_generator(
                list_name=list_name,desig_loc=desig_loc,desig_loc_login=desig_loc_login,
                extract_urls=extract_urls,prospect_db=prospect_db,visible=visible,
                what=what,login=login,crawl_normal=crawl_normal,browser=browser,
                low_ind=low_ind,high_ind=high_ind,main_thread=main_thread,
                n_threads=n_threads,n_urls_in_thread=n_urls_thread,n_iters=n_iters,
                hours=hours
           )
    )
    if out_file:
        print('saving to file')
        save_to_file_batch(list_name,desig_loc,out_file,sep_files)