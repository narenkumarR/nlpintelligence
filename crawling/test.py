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

def get_output(url,res_1,event):
    res_1['result'] = company_crawler.get_organization_details_from_linkedin_link(url)
    event.set()

company_crawler = linkedin_company_crawler.LinkedinOrganizationService()
# actions = ActionChains(company_crawler.link_parser.browser)

while no_errors<6 :
    if not urls:
        break
    ind = randint(0,len(urls)-1)
    url = urls[ind]
    logging.info('Input URL:'+url)
    try:
        time.sleep(randint(8,14))
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
        else:
            continue
        if res['Company Name']:
            res_list.append(res)
            urls.remove(url)
            with open('company_crawling_res.csv','a') as f:
                f.write(str(res)+'\n')
        no_errors = 0
    except Exception as e:
        logging.error('Error while execution, sleeping 30 seconds:',e)
        time.sleep(30)
        no_errors += 1
    if ind%5 == 0:
        time.sleep(10)
        if ind%20 == 0:
            time.sleep(20)
        if ind%50 == 0:
            time.sleep(40)
            if ind%100 == 0:
                time.sleep(300)

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
        time.sleep(randint(8,14))
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
        else:
            continue
        if res['Name']:
            res_list.append(res)
            urls.remove(url)
            with open('people_crawling_res.csv','a') as f:
                f.write(str(res)+'\n')
        no_errors = 0
    except Exception as e:
        logging.error('Error while execution, sleeping 30 seconds:',e)
        time.sleep(30)
        no_errors += 1
    if ind%5 == 0:
        time.sleep(10)
        if ind%20 == 0:
            time.sleep(20)
        if ind%50 == 0:
            time.sleep(40)
            if ind%100 == 0:
                time.sleep(300)

logging.info('Finished')
