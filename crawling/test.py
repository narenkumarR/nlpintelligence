__author__ = 'joswin'

# import pandas as pd

# orgs = pd.read_csv("odm.csv/organizations.csv")


#linkedin company page crawler
import linkedin_company_crawler
import logging
from random import randint
import pandas as pd
import time
# import linkedin_parser
from selenium.webdriver.common.action_chains import ActionChains
import multiprocessing
import threading

# link_parser = linkedin_parser.LinkedinParserSelenium()

orgs = pd.read_csv("odm.csv/organizations.csv")

logging.basicConfig(filename='company_extraction.log', level=logging.INFO,format='%(asctime)s %(message)s')
logging.info('Started')
urls = orgs['linkedin_url']
urls = urls[~urls.isnull()]
urls = list(urls[:1000])
res_list = []
no_errors = 0

company_crawler = linkedin_company_crawler.LinkedinOrganizationService()
# actions = ActionChains(company_crawler.link_parser.browser)
def get_output(url,res_1,event):
    res_1['result'] = company_crawler.get_organization_details_from_linkedin_link(url)
    event.set()


while no_errors<6 :
    if not urls:
        break
    ind = randint(0,len(urls)-1)
    url = urls[ind]
    logging.info('Input URL:'+url)
    try:
        time.sleep(randint(1,4))
        res_1 = {}
        event = threading.Event()
        # try:
        #     res = company_crawler.get_organization_details_from_linkedin_link(url)
        # except:
        #     res = company_crawler.get_organization_details_from_linkedin_link(url,use_selenium=True)
        # actions.move_by_offset(randint(0,2000),randint(0,2000))
        # actions.perform()
        # # logging.info('Result of scraping:'+str(res))
        t1 = threading.Thread(target=get_output, args=(url,res_1,event,))
        t1.daemon = True
        t1.start()
        event.wait(timeout=15)
        if res_1:
            res = res_1['result']
            urls.remove(url)
        else:
            continue
        if res['Company Name']:
            res_list.append(res)
            with open('company_crawling_res.csv','a') as f:
                f.write(str(res)+'\n')
        no_errors = 0
    except Exception as e:
        logging.error('Error while execution, sleeping 30 seconds:',e)
        time.sleep(30)
        no_errors += 1
    if ind%10 == 0:
        time.sleep(randint(8,12))
        if ind%100 == 0:
            time.sleep(randint(25,35))

logging.info('Finished')



#linkedin people page crawler
import linkedin_profile_crawler
import logging
from random import randint
import pandas as pd
import time
# import linkedin_parser
import threading

# link_parser = linkedin_parser.LinkedinParserSelenium()

people = pd.read_csv("odm.csv/people.csv")

urls = people['linkedin_url']
urls = urls[~urls.isnull()]
urls = list(urls[:1000])
res_list = []
no_errors = 0
logging.basicConfig(filename='people_extraction.log', level=logging.INFO,format='%(asctime)s %(message)s')
logging.info('Started')

people_crawler = linkedin_profile_crawler.LinkedinProfileCrawler()

def get_output(url,res_1,event):
    res_1['result'] = people_crawler.fetch_details_urlinput(url)
    event.set()

# actions = ActionChains(company_crawler.link_parser.browser)

while no_errors<6 :
    if not urls:
        break
    ind = randint(0,len(urls)-1)
    url = urls[ind]
    logging.info('Input URL:'+url)
    try:
        time.sleep(randint(1,4))
        res_1 = {}
        event = threading.Event()
        # try:
        #     res = company_crawler.get_organization_details_from_linkedin_link(url)
        # except:
        #     res = company_crawler.get_organization_details_from_linkedin_link(url,use_selenium=True)
        # actions.move_by_offset(randint(0,2000),randint(0,2000))
        # actions.perform()
        # # logging.info('Result of scraping:'+str(res))
        t1 = threading.Thread(target=get_output, args=(url,res_1,event,))
        t1.daemon = True
        t1.start()
        event.wait(timeout=15)
        if res_1:
            res = res_1['result']
            urls.remove(url)
        else:
            continue
        if res['Name']:
            res_list.append(res)
            with open('people_crawling_res.csv','a') as f:
                f.write(str(res)+'\n')
        no_errors = 0
    except Exception as e:
        logging.error('Error while execution, sleeping 30 seconds:',e)
        time.sleep(30)
        no_errors += 1
    if ind%10 == 0:
        time.sleep(randint(8,12))
        if ind%100 == 0:
            time.sleep(randint(25,35))

logging.info('Finished')


########################################threading test
import Queue
import threading

# input queue to be processed by many threads
q_in = Queue.Queue(maxsize=0)

# output queue to be processed by one thread
q_out = Queue.Queue(maxsize=0)

# number of worker threads to complete the processing
num_worker_threads = 4

# process that each worker thread will execute until the Queue is empty
def worker():
    ind = 0
    while True:
        print('ind value is :'+str(ind))
        # get item from queue, do work on it, let queue know processing is done for one item
        item = q_in.get()
        q_out.put(do_work(item))
        q_in.task_done()
        ind += 1

# squares a number and returns the number and its square
def do_work(item):
    return (item,item*item)

# another queued thread we will use to print output
def printer():
    while True:
        # get an item processed by worker threads and print the result. Let queue know item has been processed
        item = q_out.get()
        print "%d squared is : %d" % item
        q_out.task_done()

# launch all of our queued processes
def main():
    # Launches a number of worker threads to perform operations using the queue of inputs
    for i in range(num_worker_threads):
         t = threading.Thread(target=worker)
         t.daemon = True
         t.start()
    # launches a single "printer" thread to output the result (makes things neater)
    t = threading.Thread(target=printer)
    t.daemon = True
    t.start()
    # put items on the input queue (numbers to be squared)
    for item in range(40):
        q_in.put(item)
    # wait for two queues to be emptied (and workers to close)
    q_in.join()       # block until all tasks are done
    q_out.join()
    print "Processing Complete"

main()

########################################multiprocessing test
import multiprocessing
from multiprocessing import JoinableQueue as Queue

# input queue to be processed by many threads
q_in = Queue(maxsize=0)

# output queue to be processed by one thread
q_out = Queue(maxsize=0)

# number of worker threads to complete the processing
num_worker_threads = 4

# process that each worker thread will execute until the Queue is empty
def worker():
    ind = 0
    while True:
        print('ind value is :'+str(ind))
        # get item from queue, do work on it, let queue know processing is done for one item
        item = q_in.get()
        q_out.put(do_work(item))
        q_in.task_done()
        ind += 1

# squares a number and returns the number and its square
def do_work(item):
    return (item,item*item)

# another queued thread we will use to print output
def printer():
    while True:
        # get an item processed by worker threads and print the result. Let queue know item has been processed
        item = q_out.get()
        print "%d squared is : %d" % item
        q_out.task_done()

# launch all of our queued processes
def main():
    # Launches a number of worker threads to perform operations using the queue of inputs
    for i in range(num_worker_threads):
         t = multiprocessing.Process(target=worker)
         t.daemon = True
         t.start()
    # launches a single "printer" thread to output the result (makes things neater)
    t = multiprocessing.Process(target=printer)
    t.daemon = True
    t.start()
    # put items on the input queue (numbers to be squared)
    for item in range(40):
        q_in.put(item)
    # wait for two queues to be emptied (and workers to close)
    q_in.join()       # block until all tasks are done
    q_out.join()
    print "Processing Complete"

main()

####running
###getting already extracted links
# sudo pip2 install pyvirtualdisplay
# sudo apt-get install xvfb #these 2 for making firefox without displaying it
def get_finished_links(var_name='Linkedin URL',*args):
    link_list = []
    for f_name in args:
        with open(f_name,'r') as f:
            for line in f:
                tmp = eval(line)
                link_list.append(tmp[var_name])
    return list(set(link_list))


# import pandas as pd
# orgs = pd.read_csv("odm.csv/organizations.csv")
# urls = orgs['linkedin_url']
# urls = urls[~urls.isnull()]
# urls = list(set(urls))
import crawler
finished_urls = get_finished_links('Linkedin URL','crawled_res/company_crawled_13April.txt',
                         'crawled_res/company_crawled_13April_1.txt','crawled_res/company_crawled_13April_2.txt',
                         'crawled_res/company_crawled_15April.txt','crawled_res/company_crawled_15April_1.txt',
                         'crawled_res/company_crawled_15April_2.txt','crawled_res/company_crawled_18April_1.txt',
                         'crawled_res/company_crawled_19April_1.txt','crawled_res/company_crawled_19April_2.txt',
                         'crawled_res/company_crawled_20April_1.txt','crawled_res/company_crawled_20April_2.txt',
                         'crawled_res/company_crawled_20April_3.txt')


import pickle
with open('company_urls_to_crawl_18April.pkl','r') as f:
    urls = pickle.load(f)

second_urls = []
with open('companies_second_layer_urls.txt','r') as f:
    for line in f:
        second_urls.append(line[:-1])

urls = urls+second_urls
# urls = urls[:40000]
urls = list(set(urls)-set(finished_urls))
from random import shuffle
shuffle(urls)
# del orgs
cc = crawler.LinkedinCompanyCrawlerThread('Firefox',visible=False,proxy=True)
cc.run(urls,'company_crawled_21April_1.txt','company_crawling_21April_1.log',6)

# import pandas as pd
# people = pd.read_csv("odm.csv/people.csv")
# urls = people['linkedin_url']
# urls = urls[~urls.isnull()]
# urls = list(set(urls))
from random import shuffle
finished_urls = get_finished_links('Linkedin URL','crawled_res/people_crawled_12April.txt',
                         'crawled_res/people_crawled_13April.txt','crawled_res/people_crawled_13April_1.txt',
                         'crawled_res/people_crawled_13April_2.txt','crawled_res/people_crawled_15April_1.txt',
                         'crawled_res/people_crawled_15April_2.txt','crawled_res/people_crawled_18April_1.txt',
                         'crawled_res/people_crawled_18April_2.txt','crawled_res/people_crawled_19April_1.txt',
                         'crawled_res/people_crawled_19April_2.txt','crawled_res/people_crawled_20April_1.txt',
                         'crawled_res/people_crawled_20April_2.txt','crawled_res/people_crawled_20April_3.txt')

import pickle
with open('people_urls_to_crawl_18April.pkl','r') as f:
    urls = pickle.load(f)

urls = urls[:120000]
urls = list(set(urls)-set(finished_urls))
shuffle(urls)
urls = urls[:30000]
# del people
import crawler
cc = crawler.LinkedinProfileCrawlerThread('Firefox',visible=False,proxy=True)
cc.run(urls,'people_crawled_21April_1.txt','people_crawling_21April_1.log',6)

###trying with process #causing errors(storing wrong information)
#############################################################
#############################################################

import crawler_multiprocessing
import pandas as pd
orgs = pd.read_csv("odm.csv/organizations.csv")
urls = orgs['linkedin_url']
urls = urls[~urls.isnull()]
urls = list(set(urls))
finished_urls = get_finished_links('Linkedin URL','crawled_res/company_crawled_13April.txt',
                         'crawled_res/company_crawled_13April_1.txt','crawled_res/company_crawled_13April_2.txt')
urls = list(set(urls)-set(finished_urls))
del orgs
cc = crawler_multiprocessing.LinkedinCompanyCrawlerProcess('PhantomJS')
cc.run(urls,'company_crawled_15April.txt','company_crawling_15April.log',6)

import crawler_multiprocessing
import pandas as pd
people = pd.read_csv("odm.csv/people.csv")
urls = people['linkedin_url']
urls = urls[~urls.isnull()]
urls = list(set(urls))
finished_urls = get_finished_links('Linkedin URL','crawled_res/people_crawled_12April.txt',
                         'crawled_res/people_crawled_13April.txt','crawled_res/people_crawled_13April_1.txt',
                         'crawled_res/people_crawled_13April_2.txt')
urls = list(set(urls)-set(finished_urls))
del people
cc = crawler_multiprocessing.LinkedinProfileCrawlerProcess('PhantomJS')
cc.run(urls,'people_crawled_15April.txt','people_crawling_15April.log',4)

#############################################################
#############################################################
#############################################################

####removing duplicates
good_dict = {}
with open('people_crawled_15April_2.txt','r') as f:
    for i in f:
        tmp = eval(i)
        if tmp['Name'] not in good_dict:
            good_dict[tmp['Name']] = tmp

with open('people_crawled_clean.txt','w') as f:
    for i in good_dict:
        f.write(str(good_dict[i])+'\n')

#############################################################
#############################################################
#############################################################
# extracting urls from company page and people page for next level of crawling
name_list = ['crawled_res/company_crawled_13April.txt',
                         'crawled_res/company_crawled_13April_1.txt','crawled_res/company_crawled_13April_2.txt',
                         'crawled_res/company_crawled_15April.txt','crawled_res/company_crawled_15April_1.txt',
                         'crawled_res/company_crawled_15April_2.txt','crawled_res/company_crawled_18April_1.txt',
                         'crawled_res/company_crawled_19April_1.txt','crawled_res/company_crawled_19April_2.txt',
                         ]
out_file = 'companies_second_layer_urls.txt'
out_list = []
for name in name_list:
    with open(name,'r') as f_in:
        for line in f_in:
            tmp = eval(line)
            if 'Also Viewed Companies' in tmp:
                for det in tmp['Also Viewed Companies']:
                    out_list.append(det['company_linkedin_url'])

out_list = list(set(out_list))
import re
with open(out_file,'w') as f:
    for i in out_list:
        f.write(re.sub(r'\?trk=extra_biz_viewers_viewed','',i)+'\n')

#for people
name_list = ['crawled_res/people_crawled_12April.txt',
                         'crawled_res/people_crawled_13April.txt','crawled_res/people_crawled_13April_1.txt',
                         'crawled_res/people_crawled_13April_2.txt','crawled_res/people_crawled_15April_1.txt',
                         'crawled_res/people_crawled_15April_2.txt','crawled_res/people_crawled_18April_1.txt',
                         'crawled_res/people_crawled_18April_2.txt','crawled_res/people_crawled_19April_1.txt',
                         'crawled_res/people_crawled_19April_2.txt']
out_file = 'people_second_layer_urls.txt'
out_list = []
import re
for name in name_list:
    with open(name,'r') as f_in:
        for line in f_in:
            tmp = eval(line)
            if 'Related People' in tmp:
                for det in tmp['Related People']:
                    out_list.append(re.sub(r'\?trk=pub-pbmap','',det['Linkedin Page']))
            if 'Same Name People' in tmp:
                for det in tmp['Same Name People']:
                    out_list.append(re.sub(r'\?trk=prof-samename-picture','',det['Linkedin Page']))

out_list = list(set(out_list))
with open(out_file,'w') as f:
    for i in out_list:
        f.write(i+'\n')

#find people urls from companies and add it to the list