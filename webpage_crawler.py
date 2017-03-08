__author__ = 'joswin'

import datetime
import logging
import json
import pandas as pd
import re
from optparse import OptionParser
from random import shuffle
import time
import tldextract
import threading

from selenium_crawl import SeleniumParser
from utils import SoupUtils
from constants import website_column,url_validation_reg
from url_cleaner import UrlCleaner
from postgres_connect import PostgresConnect
from timeout import timeout
from Queue import Queue

logging.basicConfig(filename='logs/website_extraction_{}.log'.format(datetime.datetime.now()),
                    level=logging.INFO,format='%(asctime)s %(message)s')


class WebsiteCrawler(object):
    '''
    '''
    def __init__(self,visible=True,min_pages_per_link=3):
        '''
        :param visible:
        :param min_pages_per_link:
        :return:
        '''
        self.soup_util = SoupUtils()
        self.con = PostgresConnect()
        self.con.connect()
        self.visible = visible
        self.proxy = False
        # self.browser = BeautifulsoupCrawl(page_load_timeout=120)
        self.browser = SeleniumParser(visible=visible)
        self.url_cleaner = UrlCleaner()
        self.min_pages_per_link = min_pages_per_link
        self.social_url_searcher = re.compile(r'linkedin\.com/|facebook\.com/|twitter\.com|instagram\.com|'
                                              r'itunes|play.google|pinterest\.com|youtube\.com|vimeo\.com'
                                              r'|plus\.google\.com|tumblr.com|windowsphone\.com',re.IGNORECASE)
        self.next_level_url_priority_searcher = re.compile(r'download app|contact|career|about|shipping|handling|ordering'
                                                           r'|team|return|exchange|cancellation|pricing|delivery'
                                                           r'|features|locations|faq|question|payment|our story',re.IGNORECASE)
        self.next_level_url_searcher = re.compile(r'shop|join',re.IGNORECASE)
        self.shipping_company_regex = re.compile(r'(\b|\n|\r)(usps|us(.)?post|us(.)?postal|fed(.)?ex|dhl|blue(.)?dart'
                                         r'|United(.)?Parcel(.)?Service|Central(.)?courier|Dart(.)?Courier|Lone(.)?star'
                                         r'|TNT(.)Express|Zipments|Deutsche(.)?Post)(\b|\n|\r)',re.I)
        self.shipping_url_regex_case_sensitive = re.compile(r'(\b|\n|\r)(UPS)(\b|\n|\r)')
        self.shipping_url_regex = re.compile(r'about|faq|shipping|handling|ordering|delivery|exchange|return|question'
                                             r'|policy|policies',re.I)
        self.sep_shipping_url_regex = re.compile(r'shipping|handling|ordering|delivery|exchange|return|track(.)?order|order(.)?track',re.I)
        self.product_recom_regex = re.compile(r'Frequently Bought Together|Bought This (Item|product) Also Bought',re.I)
        self.product_url_format = re.compile(r'/product(s)?/',re.I)
        self.collections_url_format = re.compile(r'/collection(s)?/|/categories/',re.I)


    def close(self):
        '''
        :return:
        '''
        self.browser.exit()

    # @timeout(1800)
    def get_res_webpage_base_url(self,base_url):
        '''
        :param base_url:
        :return:
        '''
        feat_dic,url_sources,url_texts = {},{},{}
        base_url = self.url_cleaner.clean_url(base_url,False)
        try:
            soup = self.browser.get_soup(base_url)
        except : #if connection error, try with secure true
            logging.info('error happened with base url. Try again')
            base_url = self.url_cleaner.clean_url(base_url,True)
            try:
                soup = self.browser.get_soup(base_url)
            except:
                logging.exception('Base url crawling gave error. Returning ')
                return False,url_texts,url_sources,feat_dic
        url_sources['base_url_source'] = str(soup)
        base_page_text = self.soup_util.get_text_from_soup(soup)
        if re.search('Server not found',base_page_text,re.I):
            logging.info('The page says server not found. returning empty. url:{}'.format(base_url))
            return False,url_texts,url_sources,feat_dic
        if soup.title:
            if re.search('Something went wrong',soup.title.text,re.I):
                logging.info('The page title is something went wrong. returning empty. url:{}'.format(base_url))
                return False,url_texts,url_sources,feat_dic
            feat_dic['page_title'] = soup.title.text
        url_texts['base_url_text'] = base_page_text
        meta_text,tag_texts,url_text = self.soup_util.get_text_cleaned_format_from_soup(soup)
        url_texts['text_from_meta_base_url'],url_texts['text_from_tags_base_url'],url_texts['text_from_urls_base_url'] = meta_text,tag_texts,url_text
        # urls is of form [(url1,text1),(url2,text2),..], emails is a list of emails
        all_urls_texts, all_emails = self.soup_util.get_all_links_soupinput(soup,base_url)
        # find suitable urls and crawl them
        urls_texts_to_crawl = self.find_urls_to_crawl(all_urls_texts,base_url)
        self.crawl_urls(urls_texts_to_crawl,base_url,all_emails,all_urls_texts,url_texts,url_sources)
        product_urls_texts_to_crawl = self.find_product_urls_to_crawl(all_urls_texts,base_url)
        if product_urls_texts_to_crawl:
            feat_dic['product_urls'] = product_urls_texts_to_crawl
            self.crawl_urls(product_urls_texts_to_crawl,base_url,all_emails,all_urls_texts,url_texts,url_sources)
            try:
                feat_dic['product_recommendation'] = self.get_product_recommendation(url_texts,product_urls_texts_to_crawl)
            except:
                pass
        feat_dic['product_categories'] = self.find_product_categories(all_urls_texts,base_url)
        feat_dic['mobile_app_present'] = self.get_mobile_app_presence(all_urls_texts)
        feat_dic['cart_present'] = self.get_cart_presence(all_urls_texts)
        feat_dic['login_signup_present'],feat_dic['demo_present'],feat_dic['pricing_present'] = self.get_saas_related_values(all_urls_texts)
        feat_dic['social_urls'] = list(set([url for url,text in all_urls_texts if self.social_url_searcher.search(url)]))
        feat_dic['emails'] = list(set(all_emails))
        feat_dic['all_urls_in_website'] = all_urls_texts
        seperate_shipping_page_present,shipping_present_in_page,shipping_provider_dets = self.get_shipping_provider(url_texts)
        feat_dic['seperate_shipping_page_present'] = seperate_shipping_page_present
        feat_dic['shipping_present_in_page'] = shipping_present_in_page
        if shipping_provider_dets:
            feat_dic['shipping_providers'] = shipping_provider_dets
        # rest_url_text = re.sub(' +',' ',re.sub('[^A-Za-z0-9?!. ]+', ' ', rest_url_text))
        # base_url_text = re.sub(' +',' ',re.sub('[^A-Za-z0-9?!. ]+', ' ', base_url_text))
        return True,url_texts,url_sources,feat_dic

    def find_product_urls_to_crawl(self,all_urls_texts,base_url):
        tldextract_obj = tldextract.extract(base_url)
        base_url_domain = tldextract_obj.domain+'.'+tldextract_obj.suffix
        all_urls_texts = [i for i in all_urls_texts if base_url_domain in i[0]]
        df = pd.DataFrame(all_urls_texts,columns=['url','url_text'])
        df['url'] = df['url'].fillna('').apply(lambda x:x.strip())
        df['url_text'] = df['url_text'].fillna('').apply(lambda x:x.strip())
        df = df.drop_duplicates()
        df['url_len'] = df['url'].apply(lambda x: len(x.strip()))
        df['url_text_len'] = df['url_text'].apply(lambda x: len(x.strip()))
        urls1 = list(df.sort_values('url_text_len',ascending=False)['url'])[:5]
        urls2 = list(df.sort_values('url_len',ascending=False)['url'])[:5]
        urls3 = [url for url,text in all_urls_texts if self.product_url_format.search(url)]
        urls = list(set(urls1+urls2+urls3))
        return [(url,'') for url in urls]

    def find_product_categories(self,all_urls_texts,base_url):
        tldextract_obj = tldextract.extract(base_url)
        base_url_domain = tldextract_obj.domain+'.'+tldextract_obj.suffix
        all_urls_texts = [i for i in all_urls_texts if base_url_domain in i[0]]
        categories = [text.strip() for url,text in all_urls_texts if self.collections_url_format.search(url)]
        categories = list(set(categories))
        return categories

    def crawl_urls(self,urls_to_crawl,base_url,all_emails,all_urls_texts,url_texts,url_sources):
        '''
        :param urls_to_crawl: [(url1,url1_text),(url2,url2_text),..]
        :param base_url:
        :param all_emails:
        :param all_urls_texts:
        :param url_texts:
        :param url_sources:
        :return:
        '''
        for url,text in urls_to_crawl:
            logging.info('trying the next level url:{}'.format(url.decode('utf-8','ignore')))
            # text = text.strip()
            # get soup object
            try:
                soup = self.browser.get_soup(url)
            except:
                logging.error('Error happened in next level url:{},base_url:{}'.format(url,base_url))
                continue
            meta_text,tag_texts,url_text = self.soup_util.get_text_cleaned_format_from_soup(soup)
            page_text = self.soup_util.get_text_from_soup(soup)
            urls_tmp,mails_tmp = self.soup_util.get_all_links_soupinput(soup,base_url)
            all_emails.extend(mails_tmp)
            all_urls_texts.extend(urls_tmp)
            url_texts[url] = page_text
            url_texts['text_from_meta_'+url],url_texts['text_from_tags_'+url],url_texts['text_from_urls_'+url] = meta_text,tag_texts,url_text
            url_sources[url] = str(soup)

    def find_urls_to_crawl(self,all_urls_texts,base_url):
        '''
        :param all_urls_texts: [(url1,url1_text),(url2,url2_text),..]
        :param base_url:
        :return:
        '''
        tldextract_obj = tldextract.extract(base_url)
        base_url_domain = tldextract_obj.domain+'.'+tldextract_obj.suffix
        matching_urls = [(url.strip(),text.strip()) for url,text in all_urls_texts if (self.next_level_url_priority_searcher.search(url) or
                         self.next_level_url_priority_searcher.search(text)) and base_url_domain in url]
        matching_urls = list(set(matching_urls))
        matching_urls1 = [(url.strip(),text.strip()) for url,text in all_urls_texts if (self.next_level_url_searcher.search(url) or
                         self.next_level_url_searcher.search(text)) and base_url_domain in url]
        matching_urls1 = list(set(matching_urls1))
        shuffle(matching_urls1)
        matching_urls1 = matching_urls1[:5]
        matching_urls = list(set(matching_urls+matching_urls1))
        next_urls = [(url,text) for url,text in all_urls_texts if not ((base_url_domain not in url) or re.search('\.png$|zip$',url))]
        rest_urls = list(set(next_urls)-set(matching_urls))
        shuffle(rest_urls)
        urls_to_crawl = matching_urls + rest_urls[:min(len(rest_urls),self.min_pages_per_link)]
        return urls_to_crawl

    def get_saas_related_values(self,all_urls_texts):
        '''
        :param text:
        :return:
        '''
        # find if there is a login page
        login_signup_present = True if [1 for url,text in all_urls_texts if re.search(r'\blogin\b|\bsign(.)?up\b|my(.)account',url,re.IGNORECASE)
                    or re.search(r'\blogin\b|\bsign(.)?up\b|\bsign(.)?in|my(.)account',text,re.IGNORECASE)] else False
        # find if there is a demo page
        demo_present = True if [1 for url,text in all_urls_texts if re.search(r'\bdemo\b|\btrial\b|\btry( for) free\b',url,re.IGNORECASE)
                    or re.search(r'\bdemo\b|\btrial\b',text,re.IGNORECASE)] else False
        # find if there is a pricing page
        pricing_present = True if [1 for url,text in all_urls_texts if re.search(r'\bplans\b|\bpricing\b|\bpayment\b|billing',url,re.IGNORECASE)
                    or re.search(r'\bpricing\b|\bpayment\b|billing',text,re.IGNORECASE)] else False
        return login_signup_present,demo_present,pricing_present

    def get_mobile_app_presence(self,all_urls_texts):
        '''
        :param urls:
        :return:
        '''
        # find if there is a login page
        mobile_app_present = True if [1 for url,text in all_urls_texts if re.search(r'itunes|play.google',url,re.IGNORECASE)
                    or re.search(r'download app|app download|get app',text,re.IGNORECASE)] else False
        return mobile_app_present

    def get_cart_presence(self,all_urls_texts):
        cart_present = True if [1 for url,text in all_urls_texts if re.search(r'cart|my(.)?bag|checkout|basket',url,re.IGNORECASE)
                    or re.search(r'cart|my(.)bag|checkout|basket',text,re.IGNORECASE)] else False
        return cart_present

    def get_shipping_provider(self,url_texts_dic):
        '''
        :param url_texts:
        :return:
        '''
        shipping_provider_list = []
        seperate_shipping_page_present = False
        shipping_present_in_page = False
        for key in url_texts_dic:
            if self.sep_shipping_url_regex.search(key):
                seperate_shipping_page_present = True
            if self.shipping_url_regex.search(key):
                text = url_texts_dic[key]
                if re.search('ship',text,re.I):
                    shipping_provider_list.extend([text[match.start():match.end()].strip() for match in self.shipping_company_regex.finditer(text)])
                    shipping_provider_list.extend([text[match.start():match.end()].strip() for match in self.shipping_url_regex_case_sensitive.finditer(text)])
                if re.search('shipping',text,re.I):
                    shipping_present_in_page = True
        return seperate_shipping_page_present,shipping_present_in_page,'|'.join(set(shipping_provider_list))

    def get_product_recommendation(self,url_texts_dic,product_urls_texts):
        '''
        :param url_texts:
        :return:
        '''
        product_urls_reg = re.compile('|'.join([re.sub('[^a-zA-Z0-9]','.',i[0]) for i in product_urls_texts]),re.I)
        product_recom_present = False
        for key in url_texts_dic:
            if not product_urls_reg.search(key):
                continue
            if self.product_recom_regex.search(url_texts_dic[key]):
                product_recom_present = True
                break #can change this logic to be present more than x% of pages also
        return product_recom_present

    # @timeout(1800)
    def get_res_webpage_base(self,base_url):
        '''
        :param base_url:
        :return:
        '''
        try:
            success,url_texts,url_sources,feat_dic = self.get_res_webpage_base_url(
                base_url=base_url
            )
            if success:
                logging.info('Extracted info for url:{}'.format(base_url))
                self.save_to_table(base_url=base_url,url_texts=url_texts,url_sources=url_sources,feat_dic=feat_dic)
                return True
            else:
                logging.info('Extraction failed for url:{}'.format(base_url))
                raise ValueError('Error raised for restarting the browser since get_res_webpage_base_url returned False')
        except:
            logging.exception('Error happened in get_res_webpage_base for url:{}'.format(base_url))
            return False

    def save_to_table(self,base_url,url_texts,url_sources,feat_dic):
        '''

        create table crawler.webpage_texts (
            id serial primary key,
            url text,
            all_page_text text,
            home_page_text text,
            misc_details json,
            created_on timestamp default current_timestamp,
            updated_on timestamp default current_timestamp
        );
        :param base_url_text:
        :param rest_url_text:
        :param kwargs:
        :return:
        '''
        tldextract_obj = tldextract.extract(base_url)
        domain = tldextract_obj.domain+'.'+tldextract_obj.suffix
        self.con.get_cursor()
        query = " insert into webpage_texts (url,domain,all_page_text,page_sources,misc_details) values " \
                " (%s,%s,%s,%s,%s)"
        self.con.cursor.execute(query,(base_url,domain,json.dumps(url_texts),json.dumps(url_sources),json.dumps(feat_dic),))
        self.con.commit()
        self.con.close_cursor()


class WebCrawlerScheduler(object):
    def __init__(self):
        pass

    def run_website_crawl_threaded(self,in_queue,finished_queue,n_threads=3,visible=False,min_pages=3):
        '''
        :param in_queue:
        :param finished_queue:
        :param n_threads:
        :param visible:
        :param min_pages:
        :return:
        '''
        logging.info('starting crawling for websites in table:{}'.format(table_name))
        def worker():
            ind = 0
            wpe = WebsiteCrawler(visible=visible,min_pages_per_link=min_pages)
            while not in_queue.empty():
                website = in_queue.get()
                try:
                    if not website:
                        continue
                    logging.info('Trying for url: {}'.format(website))
                    if not website:
                        pass
                    website_clean = wpe.url_cleaner.clean_url(website,False)
                    if url_validation_reg.search(website_clean):
                        ind += 1
                        success = wpe.get_res_webpage_base(base_url=website_clean)
                        if success:
                            finished_queue.put((website,True))
                        if not success :
                            finished_queue.put((website,False))
                            logging.info('error happened while trying for website:{}. Restarting browser'.format(website))
                            wpe.browser.exit()
                            wpe.browser.start_browser(visible=wpe.visible)
                        if ind % 50 == 0:
                            ind = 0
                            wpe.browser.exit()
                            wpe.browser.start_browser(visible=wpe.visible)
                except:
                    logging.exception('Some error happened. continuing the run')
                    try:
                        wpe.browser.exit()
                    except:
                        pass
                    wpe.browser.start_browser(visible=wpe.visible)
                in_queue.task_done()
            wpe.browser.exit()
            wpe.con.close_connection()
        logging.info('workers starting')
        for i in range(n_threads):
            worker_tmp = threading.Thread(target=worker)
            worker_tmp.setDaemon(True)
            worker_tmp.start()
        logging.info('in_queue join')
        in_queue.join()
        logging.info('in_queue join completed')
        time.sleep(60)
        self.crawler_running = False

    def run_website_crawl_table_input(self,table_name,n_threads=3,visible=False,min_pages=3):
        '''
        :param table_name:
        :param n_threads:
        :param visible:
        :param min_pages:
        :return:
        '''
        logging.info('run_website_crawl_table_input started for table:{}'.format(table_name))
        con = PostgresConnect()
        con.connect()
        con.cursor.execute("select distinct domain from {} where extraction_tried='f' ".format(table_name))
        websites = con.cursor.fetchall()
        websites = [i[0] for i in websites]
        in_queue = Queue(maxsize=0)
        finished_queue = Queue(maxsize=0)
        logging.info('no of websites for which crawling will be done:{}'.format(len(websites)))
        for website in websites:
            in_queue.put(website)
        self.crawler_running = True
        t = threading.Thread(target=self.run_website_crawl_threaded,args=(in_queue,finished_queue,n_threads,visible,min_pages,))
        t.setDaemon(True)
        t.start()
        logging.info('processing finished_queue')
        while self.crawler_running:
            if finished_queue.empty():
                continue
            finished_domain,success = finished_queue.get()
            if success:
                con.cursor.execute("update {} set extraction_tried= 't',extraction_success = 't' "
                                   " where domain=%s".format(table_name),(finished_domain,))
            else:
                con.cursor.execute("update {} set extraction_tried='t',extraction_success = 'f' "
                                   " where domain=%s".format(table_name),(finished_domain,))
            con.commit()
            time.sleep(5)
        con.close_connection()
        logging.info('completed run_website_crawl_table_input')

    def run_website_crawl_csv_input(self,website_file,table_name,n_threads=3,visible=False,min_pages=3):
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
        self.run_website_crawl_table_input(table_name,n_threads,visible,min_pages)

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
    optparser.add_option('-n', '--n_threads',
                         dest='n_threads',
                         help='n_threads',
                         default=3,type='int')

    (options, args) = optparser.parse_args()
    website_file = options.website_file
    table_name = options.table_name
    visible = options.visible
    min_pages = options.minpages
    n_threads = options.n_threads
    logging.info('started crawling')
    crawler_scheduler = WebCrawlerScheduler()
    if website_file:
        crawler_scheduler.run_website_crawl_csv_input(website_file,table_name,n_threads=n_threads,visible=visible,min_pages=min_pages)
    elif table_name:
        crawler_scheduler.run_website_crawl_table_input(table_name,n_threads,visible,min_pages)
    logging.info('completed crawling')
