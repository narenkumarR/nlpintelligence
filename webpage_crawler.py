__author__ = 'joswin'

import datetime
import logging
import json
import pandas as pd
import re
from optparse import OptionParser
from random import shuffle
import tldextract

from selenium_crawl import SeleniumParser
from utils import SoupUtils
from constants import website_column,url_validation_reg
from url_cleaner import UrlCleaner
from postgres_connect import PostgresConnect
from timeout import timeout

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
                                                           r'|team|join|shop|return|exchange|cancellation|pricing|delivery'
                                                           r'|features|locations|faq|question|payment|our story',re.IGNORECASE)
        self.shipping_company_regex = re.compile(r'(\b|\n|\r)(usps|us(.)?post|us(.)?postal|fed(.)?ex|dhl|blue(.)?dart'
                                         r'|United(.)?Parcel(.)?Service|Central(.)?courier|Dart(.)?Courier|Lone(.)?star'
                                         r'|TNT(.)Express|Zipments|Deutsche(.)?Post)(\b|\n|\r)',re.I)
        self.shipping_url_regex_case_sensitive = re.compile(r'(\b|\n|\r)(UPS)(\b|\n|\r)')
        self.shipping_url_regex = re.compile(r'about|faq|shipping|handling|ordering|delivery|exchange|return|question'
                                             r'|policy|policies',re.I)
        self.sep_shipping_url_regex = re.compile(r'shipping|handling|ordering|delivery|exchange|return',re.I)


    def close(self):
        '''
        :return:
        '''
        self.browser.exit()


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
            base_url = self.url_cleaner.clean_url(base_url,True)
            soup = self.browser.get_soup(base_url)
        if re.search('Something went wrong',soup.title,re.I):
            logging.info('The page title is something went wrong. returning empty. url:{}'.format(base_url))
            return False,url_texts,url_sources,feat_dic
        url_sources['base_url_source'] = str(soup)
        base_page_text = self.soup_util.get_text_from_soup(soup)
        if re.search('Server not found',base_page_text,re.I):
            logging.info('The page says server not found. returning empty. url:{}'.format(base_url))
            return False,url_texts,url_sources,feat_dic
        url_texts['base_url_text'] = base_page_text
        meta_text,tag_texts,url_text = self.soup_util.get_text_cleaned_format_from_soup(soup)
        url_texts['text_from_meta_base_url'],url_texts['text_from_tags_base_url'],url_texts['text_from_urls_base_url'] = meta_text,tag_texts,url_text
        # urls is of form [(url1,text1),(url2,text2),..], emails is a list of emails
        all_urls, all_emails = self.soup_util.get_all_links_soupinput(soup,base_url)
        # find suitable urls and crawl them
        tldextract_obj = tldextract.extract(base_url)
        base_url_domain = tldextract_obj.domain+'.'+tldextract_obj.suffix
        matching_urls = [(url,text) for url,text in all_urls if (self.next_level_url_priority_searcher.search(url) or
                         self.next_level_url_priority_searcher.search(text)) and base_url_domain in url]
        matching_urls = list(set(matching_urls))
        next_urls = [(url,text) for url,text in all_urls if not ((base_url_domain not in url) or re.search('\.png$|zip$',url))]
        rest_urls = list(set(next_urls)-set(matching_urls))
        shuffle(rest_urls)
        urls_to_crawl = matching_urls + rest_urls[:min(len(rest_urls),self.min_pages_per_link)]

        for url,text in urls_to_crawl:
            logging.info('trying the next level url:{}'.format(url.decode('utf-8','ignore')))
            text = text.strip()
            # get soup object
            soup = self.browser.get_soup(url)
            meta_text,tag_texts,url_text = self.soup_util.get_text_cleaned_format_from_soup(soup)
            page_text = self.soup_util.get_text_from_soup(soup)
            urls_tmp,mails_tmp = self.soup_util.get_all_links_soupinput(soup,base_url)
            all_emails.extend(mails_tmp)
            all_urls.extend(urls_tmp)
            url_texts[text] = page_text
            url_texts['text_from_meta_'+text],url_texts['text_from_tags_'+text],url_texts['text_from_urls_'+text] = meta_text,tag_texts,url_text
            url_sources[text] = str(soup)
        feat_dic['mobile_app_present'] = self.get_mobile_app_presence(all_urls)
        feat_dic['cart_present'] = self.get_cart_presence(all_urls)
        feat_dic['login_signup_present'],feat_dic['demo_present'],feat_dic['pricing_present'] = self.get_saas_related_values(all_urls)
        feat_dic['social_urls'] = list(set([url for url,text in all_urls if self.social_url_searcher.search(url)]))
        feat_dic['emails'] = list(set(all_emails))
        feat_dic['all_urls_in_website'] = all_urls
        seperate_shipping_page_present,shipping_provider_dets = self.get_shipping_provider(url_texts)
        feat_dic['seperate_shipping_page_present'] = seperate_shipping_page_present
        if shipping_provider_dets:
            feat_dic['shipping_providers'] = shipping_provider_dets
        # rest_url_text = re.sub(' +',' ',re.sub('[^A-Za-z0-9?!. ]+', ' ', rest_url_text))
        # base_url_text = re.sub(' +',' ',re.sub('[^A-Za-z0-9?!. ]+', ' ', base_url_text))
        return True,url_texts,url_sources,feat_dic

    def get_saas_related_values(self,urls):
        '''
        :param text:
        :return:
        '''
        # find if there is a login page
        login_signup_present = True if [1 for url,text in urls if re.search(r'\blogin\b|\bsign(.)?up\b|my(.)account',url,re.IGNORECASE)
                    or re.search(r'\blogin\b|\bsign(.)?up\b|\bsign(.)?in|my(.)account',text,re.IGNORECASE)] else False
        # find if there is a demo page
        demo_present = True if [1 for url,text in urls if re.search(r'\bdemo\b|\btrial\b|\btry( for) free\b',url,re.IGNORECASE)
                    or re.search(r'\bdemo\b|\btrial\b',text,re.IGNORECASE)] else False
        # find if there is a pricing page
        pricing_present = True if [1 for url,text in urls if re.search(r'\bplans\b|\bpricing\b|\bpayment\b|billing',url,re.IGNORECASE)
                    or re.search(r'\bpricing\b|\bpayment\b|billing',text,re.IGNORECASE)] else False
        return login_signup_present,demo_present,pricing_present

    def get_mobile_app_presence(self,urls):
        '''
        :param urls:
        :return:
        '''
        # find if there is a login page
        mobile_app_present = True if [1 for url,text in urls if re.search(r'itunes|play.google',url,re.IGNORECASE)
                    or re.search(r'download app|app download|get app',text,re.IGNORECASE)] else False
        return mobile_app_present

    def get_cart_presence(self,urls):
        cart_present = True if [1 for url,text in urls if re.search(r'cart|my(.)?bag|checkout|basket',url,re.IGNORECASE)
                    or re.search(r'cart|my(.)bag|checkout|basket',text,re.IGNORECASE)] else False
        return cart_present

    def get_shipping_provider(self,url_texts_dic):
        '''
        :param url_texts:
        :return:
        '''
        shipping_provider_list = []
        seperate_shipping_page_present = False
        for key in url_texts_dic:
            if self.sep_shipping_url_regex.search(key):
                seperate_shipping_page_present = True
            if self.shipping_url_regex.search(key):
                text = url_texts_dic[key]
                if re.search('ship',text,re.I):
                    shipping_provider_list.extend([text[match.start():match.end()].strip() for match in self.shipping_company_regex.finditer(text)])
                    shipping_provider_list.extend([text[match.start():match.end()].strip() for match in self.shipping_url_regex_case_sensitive.finditer(text)])
        return seperate_shipping_page_present,'|'.join(set(shipping_provider_list))

    @timeout(1800)
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
            else:
                logging.info('Extraction failed for url:{}'.format(base_url))
                raise ValueError('Error raised for restarting the browser')
        except:
            logging.exception('Error happened while trying url: {}'.format(base_url))
            self.browser.exit()
            self.browser.start_browser(visible=self.visible)

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

    def search_webpage_csv_input(self,websites_loc):
        '''
        :param websites_loc:
        :param search_wts_loc:
        :param out_loc:
        :return:
        '''
        df = pd.read_csv(websites_loc)
        df = df.fillna('')
        websites = list(df[website_column])
        ind = 0
        for website in websites:
            try:
                if not website:
                    continue
                ind += 1
                logging.info('Trying for url: {}'.format(website))
                if not website:
                    pass
                website = self.url_cleaner.clean_url(website,False)
                if url_validation_reg.search(website):
                    self.get_res_webpage_base(base_url=website)
                if ind % 50 == 0:
                    self.browser.exit()
                    self.browser.start_browser(visible=self.visible)
            except:
                logging.exception('Some error happened. continuing the run')
                try:
                    self.browser.exit()
                except:
                    pass
                self.browser.start_browser(visible=self.visible)


if __name__ == "__main__":
    optparser = OptionParser()
    optparser.add_option('-w', '--websites',
                         dest='website_file',
                         help='location of websites csv file',
                         default=None)
    optparser.add_option('-v', '--visible',
                         dest='visible',
                         help='if 1 visible, if 0 not visible',
                         default=0,type='int')
    optparser.add_option('-m', '--minpages',
                         dest='minpages',
                         help='no of pages to crawl within a url',
                         default=3,type='int')

    (options, args) = optparser.parse_args()
    website_file = options.website_file
    visible = options.visible
    min_pages = options.minpages
    wpe = WebsiteCrawler(visible=visible,min_pages_per_link=min_pages)
    logging.info('started crawling')
    wpe.search_webpage_csv_input(website_file)
    logging.info('completed crawling')
    wpe.browser.exit()