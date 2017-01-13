__author__ = 'akshaya'
'''
This is the generic code to scrape a given website and collect the below list of Information
a. All Links
b. All Social Links : LinkedIn,Facebook,and so on
c. Emails if any
d. Word Counts
'''

from selenium_crawl import SeleniumParser
from url_cleaner import UrlCleaner
from utils import SoupUtils
import nltk
from random import shuffle
import re
import codecs
import configReader as cfg
import mongodbHelper as mongo
from urlparse import urlparse
from optparse import OptionParser
from urlparse import urlunparse
import phonenumbers as ph
import logging
import datetime
from nltk.corpus import stopwords
from nltk.stem import SnowballStemmer
from nltk.stem.lancaster import LancasterStemmer
from bs_crawl import BeautifulsoupCrawl as bsCrawl
import sys,csv

snowball_stemmer = SnowballStemmer('english')
now = datetime.datetime.now()
logfile = '../logs/webscraper_{}'.format(now.strftime("%Y_%m_%d_%H_%M_%S"))
logging.basicConfig(filename=logfile, level=logging.INFO,format='%(asctime)s %(message)s')
class WebScraper(object):

    def __init__(self,visible=True,min_pages_per_link=10,most_common=20,useSelenium=True):

        self.soup_util = SoupUtils()
        self.visible = visible
        if useSelenium == True:
            self.crawler = SeleniumParser(visible=self.visible)
        self.url_cleaner = UrlCleaner()
        self.min_pages_per_link = min_pages_per_link
        self.most_common = most_common
        self.default_stopwords = set(nltk.corpus.stopwords.words('english'))
        stopwords_file = './stopwords.txt'
        stop_words = set(stopwords.words('english'))
        custom_stopwords = set(codecs.open(stopwords_file, 'r', 'utf-8').read().splitlines())

        self.default_stopwords = self.default_stopwords | custom_stopwords | stop_words
        self.db = cfg.readConfig()
        self.mg = mongo.MongodbHelper()

        # Offsets for getting Context of Phone Numbers
        self.leftOffset = 40
        self.rightOffset = 40

        # Whether to use Selenium or not
        self.useSelenium = useSelenium

    '''
    To get the HTTP Response either through Selenium or URLLib
    '''
    def getSoup(self,url):
        if self.useSelenium:
            return self.crawler.get_soup(url=url)
        else:
            return bsCrawl.get_soup(url)

    '''
    This is the function to scrape the given website
    Base URL to start with is passed as a parameter
    '''
    def scrape_page_base(self,base_url):

        base_url = self.url_cleaner.clean_url(base_url, False)
        try:
            soup = self.getSoup(base_url)
            full_text = self.soup_util.get_text_from_soup(soup)
            urls, emails,maps = self.soup_util.get_all_links_soupinput(soup, base_url)
            urls = [(url, text) for url, text in urls if
                    not ((not base_url in url) or re.search('\.png$|\.jpg$|\.pdf$|\.jpeg$|\.gif$', url) or 'login' in url)]
            shuffle(urls)

            contact_url = None
            for url,text in urls:
                if 'contact' in url:
                    contact_url = url
                    break

            print("Check {} {} {}".format(len(urls),self.min_pages_per_link,min(len(urls), self.min_pages_per_link)))
            for ind in range(min(len(urls), self.min_pages_per_link)):
                url, text = urls[ind]
                if (not base_url in url) or re.search('\.png$', url) or 'login' in url:
                    continue
                try:
                    soup = self.getSoup(url)
                    text = self.soup_util.get_text_from_soup(soup)
                    full_text = ' '.join([full_text,text])
                    urls_tmp, mails_tmp,map_loc = self.soup_util.get_all_links_soupinput(soup, base_url)
                    emails.extend(mails_tmp)
                    urls.extend(urls_tmp)
                    maps.extend(map_loc)
                except:
                    logging.info("Unexpected error: {}".format(sys.exc_info()[0]))
                    logging.info("Could not scrape url {}".format(url))
                    pass

            # Mandatorily parse Contact Page if one exists
            '''
            if contact_url is not None:
                try:
                    soup = self.getSoup(contact_url)
                    text = self.soup_util.get_text_from_soup(soup)
                    full_text = ' '.join([full_text, text])
                    urls_tmp, mails_tmp, map_loc = self.soup_util.get_all_links_soupinput(soup, base_url)
                    emails.extend(mails_tmp)
                    urls.extend(urls_tmp)
                    maps.extend(map_loc)
                except:
                    logging.info("Unexpected error: {}".format(sys.exc_info()[0]))
                    logging.info("Could not scrape url {}".format(url))
                    pass


            out_url_searcher = re.compile(r'linkedin|facebook|twitter|youtube|instagram|plus|pinterest', re.IGNORECASE)
            urls_all = list(set([url for url,text in urls]))
            social_links = list(set([url for url, text in urls if out_url_searcher.search(url)]))
            emails = list(set(emails))
            maps = list(set(maps))
            '''

            # Get Word Counts
            word_counts = self.get_word_counts(full_text)

            # Get Phone Numbers if possible
            '''
            phone_nums,phoneContext = self.getPhoneNumbers(full_text)
            phone_nums = list(set(phone_nums))
            if len(phone_nums) > 20:
                phone_nums = phone_nums[0:20]
            '''
            return True,None,None,None,word_counts,None,None,None
        except:
            logging.info("Unexpected error: {}".format(sys.exc_info()[0]))
            logging.info("Could not scrape url {}".format(start_url))
            return False,None,None,None,"",None,None,None

    '''
    To get the word counts from the given page
    '''
    def get_word_counts(self,text):

        words = nltk.word_tokenize(text)
        words = set(words)

        words = [re.sub(r'\d+', '', word) for word in words if len(word) > 1]
        words = [word for word in words if not word.isnumeric()]

        # Lowercase all words (default_stopwords are lowercase too)
        words = [word.lower() for word in words]

        #removing numbers
        words = [word for word in words if not word.isdigit()]

        # Remove stopwords
        words = [word for word in words if word not in self.default_stopwords]

        # Stemming
        st = LancasterStemmer()
        words = [st.stem(word) for word in words if len(word)>2]
        # Calculate frequency distribution
        #fdist = nltk.FreqDist(words)
        # Remove single-character tokens (mostly punctuation)
        words = [word for word in words if len(word) > 2]

        #return fdist.most_common(self.most_common)

        return ";".join(words)

    '''
    To get any phone number looking fragments
    Currently , regions being used are : US,CA,GB,FR,SG,AU,ZZ
    '''
    def getPhoneNumbers(self,text):

        phoneNums = []
        phoneContext = []
        regionList = ['US','CA','GB','FR','SG','AU','ZZ','CN','IN']
        try:
            for region in regionList:
                for match in ph.PhoneNumberMatcher(text,region):
                    left = match.start
                    right = match.end
                    left = max(0,left-self.leftOffset)
                    right = min(len(text)-1,right+self.rightOffset)
                    num = ph.format_number(match.number, ph.PhoneNumberFormat.NATIONAL)
                    phoneNums.append(num)
                    phContext = text[left,right]
                    phoneContext.append(phContext)
                    phoneContext.append(match.raw_string)
        except:
            pass

        return phoneNums,phoneContext



    '''
    To automatically close the browser when done
    '''
    def closeBrowser(self):
        self.crawler.exit()

    '''
    To get the List of Publisher Sites to be crawled
    '''
    def getPublisherList(self,recLimit=None):

        if recLimit is not None:
            items = self.mg.getPublisherSites(recLimit)
        else:
            items = self.mg.getPublisherSites()

        return items

    '''
    To add the Publisher Data into the DB
    '''
    def addToDB(self,obId,pubdata):
        pubId = self.mg.addPublisherData(pubdata)

        self.updateAppPublisherStatus(obId=obId)
        return pubId

    '''
    To update the App indicating its publisher page has been scraped
    '''
    def updateAppPublisherStatus(self,obId):
        data = {}
        data['publisher_scraped'] = True
        self.mg.updateAppDetail(obId=obId, data=data)
        print("Updated App Publisher Scrape Status {}".format(obId))


    '''
    To check if domain already visited
    '''
    def isPublisherDone(self,pubDomain):

        return self.mg.checkIfPublisherDataExists(pubDomain=pubDomain)




'''
To get the domain portion from the company/publisher site
'''
def getDomainPortion(url):
    parts = urlparse(url)

    # Return the hostname
    return parts.scheme,parts.netloc,parts.path


if __name__ == "__main__":

    optparser = OptionParser()
    optparser.add_option('-v', '--visible',
                         dest='visible',
                         help='Browser should be visible or not',
                         default=False)
    optparser.add_option('-d', '--depth',
                         dest='depth',
                         help='Minimum pages per link',
                         default=15)
    optparser.add_option('-l', '--limit',
                         dest='reclimit',
                         help='No of records to process from DB',
                         default=100)

    optparser.add_option('-u', '--useselenium',
                         dest='useSelenium',
                         help='Whether to use Selenium or not ',
                         default=False)

    optparser.add_option('-i', '--inputFile',
                         dest='inputFile',
                         help='To read data from Input File instead of DB',
                         default=None)
    (options, args) = optparser.parse_args()
    visible = options.visible
    depth = int(options.depth)
    reclimit = options.reclimit
    useSelenium = bool(options.useSelenium)
    inputFile = options.inputFile
    batchnum = 0
    hasmore = True
    url_cleaner = UrlCleaner()
    print("Args are {} {} {} ".format(visible,depth,inputFile))

    if inputFile is not None:
        hasmore = False
    while hasmore:

        batchnum = batchnum + 1
        hasmore = False
        ws = WebScraper(visible=visible, min_pages_per_link=depth,useSelenium=useSelenium)
        itemList = []
        print("Fetching the Publisher URLS ")
        logging.info("Fetching the Publisher URLS ")
        for item in ws.getPublisherList(recLimit=reclimit):
            hasmore = True
            tmp = {}
            tmp['publisher_home'] = item['publisher_home']
            tmp['publisher'] = item['publisher']
            tmp['_id'] = item['_id']
            itemList.append(tmp)

        print("Done with fetching {}".format(len(itemList)))
        logging.info("Done with fetching {}".format(len(itemList)))
        for item in itemList:
            start_url = item['publisher_home']
            start_url = url_cleaner.clean_url(start_url,secure=False)
            scheme,domain,path = getDomainPortion(start_url)
            logging.info('URL Parse {} {} {}'.format(scheme,domain,path))
            print('URL Parse {} {} {}'.format(scheme, domain, path))
            if domain == '':
                start_url = urlunparse((scheme,domain,path,'','',''))
            else:
                start_url = urlunparse((scheme, domain,'', '', '', ''))

            # Check if domain already visited
            if ws.isPublisherDone(pubDomain=domain):
                ws.updateAppPublisherStatus(obId=item['_id'])
                continue

            # Check if facebook is present in the domain
            if 'facebook' in start_url:
                ws.updateAppPublisherStatus(obId=item['_id'])
                continue

            print("Scraping {}".format(start_url))
            try:
                status,social_links,all_urls,emails,word_counts,phone_nums,phone_context,maps = ws.scrape_page_base(start_url)
                if not status:
                    ws.updateAppPublisherStatus(obId=item['_id'])
                    continue
                pubData = {}
                pubData['name'] = item['publisher']
                pubData['company_website'] = item['publisher_home']
                pubData['social_links'] = social_links
                pubData['emails'] = emails
                pubData['word_counts'] = word_counts
                pubData['phone_nums'] = phone_nums
                pubData['company_domain'] = domain
                pubData['phone_context'] = phone_context
                pubData['maps'] = maps
                pubId = ws.addToDB(obId=item['_id'], pubdata=pubData)
                print("Pub Domain : {}".format(pubData['company_domain']))
                print("Word Count Len {}".format(len(pubData['word_counts'])))
                print("Emails Len {}".format(len(pubData['emails'])))
                print("Social Links Len {}".format(pubData['social_links']))
                print("Added to the DB {}".format(pubId))
            except:
                print "Unexpected error:", sys.exc_info()[0]
                logging.info("Unexpected error: {}".format(sys.exc_info()[0]))
                logging.info("Could not scrape url {}".format(start_url))
                print("Scraping Error for {}".format(start_url))
        logging.info("Completed Batch {}".format(batchnum))
        logging.info("Starting with next Batch {}".format(batchnum))
        print("Completed Batch {}".format(batchnum))
        print("Starting with next Batch {}".format(batchnum))
        ws.closeBrowser()

    # Reading the List from Input CSV File
    ws = WebScraper(visible=visible, min_pages_per_link=depth, useSelenium=useSelenium)
    outRows = []
    with open(inputFile,'rb') as fin:
        rows = csv.reader(fin)
        rows.next()
        for row in rows:
            tmp = []
            tmp.append(row[0].encode("utf-8"))
            start_url = row[0]
            start_url = url_cleaner.clean_url(start_url, secure=False)
            print("Scraping {}".format(start_url))
            try:
                status, social_links, all_urls, emails, word_counts, phone_nums, phone_context, maps = ws.scrape_page_base(start_url)
                tmp.append(status)
                '''
                tmp.append(social_links)
                tmp.append(emails)
                #outRows.append(word_counts)
                tmp.append(phone_nums)
                tmp.append(phone_context)
                tmp.append(maps)
                '''
                tmp.append(word_counts.encode("utf-8"))
                outRows.append(tmp)
            except:
                print("Scraping Error {}".format(start_url))
                outRows.append(tmp)
    fin.close()
    if useSelenium == True:
        ws.closeBrowser()
    outFile = inputFile.replace(".csv","_out.csv")
    with open(outFile,mode="w") as fout:
        csvOut = csv.writer(fout)
        for row in outRows:
            csvOut.writerow(row)


