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



import Queue
import threading

# input queue to be processed by many threads
q_in = Queue.Queue(maxsize=0)

# output queue to be processed by one thread
q_out = Queue.Queue(maxsize=0)

# number of worker threads to complete the processing
num_worker_threads = 2

# process that each worker thread will execute until the Queue is empty
def worker():
    while True:
        # get item from queue, do work on it, let queue know processing is done for one item
        item = q_in.get()
        q_out.put(do_work(item))
        q_in.task_done()

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
    for item in range(1000):
        q_in.put(item)
    # wait for two queues to be emptied (and workers to close)
    q_in.join()       # block until all tasks are done
    q_out.join()
    print "Processing Complete"

main()


####running

import crawler
import pandas as pd
orgs = pd.read_csv("odm.csv/organizations.csv")
urls = orgs['linkedin_url']
urls = urls[~urls.isnull()]
urls = list(urls[20000:30000])
urls = list(set(urls))
del orgs
cc = crawler.LinkedinCompanyCrawlerThread()
cc.run(urls,'company_crawled_13April.txt','company_crawling_13April.log')

import crawler
import pandas as pd
people = pd.read_csv("odm.csv/people.csv")
urls = people['linkedin_url']
urls = urls[~urls.isnull()]
urls = list(urls[20000:30000])
urls = list(set(urls))
del people
cc = crawler.LinkedinProfileCrawlerThread()
cc.run(urls,'people_crawled_13April.txt','people_crawling_13April.log')
