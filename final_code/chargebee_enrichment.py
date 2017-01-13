__author__ = 'joswin'

import logging
import nltk
import pandas as pd
import pickle
import re
from itertools import izip
from optparse import OptionParser
from random import shuffle
import csv

from requests.exceptions import ConnectionError

from bs_crawl import BeautifulsoupCrawl
from selenium_crawl import SeleniumParser
from utils import SoupUtils
from constants import website_column,company_linkedin_col,id_col,search_text_column,search_text_weight_column,url_validation_reg
from url_cleaner import UrlCleaner
from linkedin_dets_extractor import CmpnyLinkedinExtractor

logging.basicConfig(filename='logs/website_extraction.log', level=logging.INFO,format='%(asctime)s %(message)s')

class ChargebeeWebSearcher(object):
    '''
    '''
    def __init__(self,search_wts_loc,visible=True,min_pages_per_link=0,search_linkedin=False):
        '''
        :param visible:
        :param min_pages_per_link:
        :return:
        '''
        self.soup_util = SoupUtils()
        self.visible = visible
        self.proxy = False
        self.login_linkedin = False
        # self.browser = BeautifulsoupCrawl(page_load_timeout=120)
        self.browser = SeleniumParser(visible=visible)
        self.url_cleaner = UrlCleaner()
        self.min_pages_per_link = min_pages_per_link
        self.social_url_searcher = re.compile(r'linkedin\.com/|facebook\.com/|twitter\.com',re.IGNORECASE)
        if search_linkedin:
            self.lkdn_parser = CmpnyLinkedinExtractor(visible=self.visible,proxy=self.proxy,login=self.login_linkedin)
        else:
            self.lkdn_parser = False
        search_texts_df = pd.read_csv(search_wts_loc,sep=None)
        search_wrds_list = list(search_texts_df[search_text_column])
        # create regex with search words. links having these keywords will be having priority
        search_wrds_weights_list = list(search_texts_df[search_text_weight_column])
        # regular expression to match words
        search_reg_text = r'\b' + r'\b|\b'.join(search_wrds_list) + r'\b'
        self.search_reg = re.compile(search_reg_text,re.IGNORECASE)
        self.keyword_regex = self.search_reg
        # matcher dictionary.. this is used to count occurances of each match
        self.matched_dic = {}
        if search_wrds_weights_list:
            for text,weight in izip(search_wrds_list,search_wrds_weights_list):
                self.matched_dic[r'\b'+text.lower()+r'\b'] = weight
        else:
            for text in search_wrds_list:
                self.matched_dic[r'\b'+text.lower()+r'\b'] = 1

    def close(self):
        '''
        :return:
        '''
        self.browser.exit()
        if self.lkdn_parser:
            try:
                self.lkdn_parser.exit()
            except:
                pass

    def search_webpage_single(self,search_reg,matched_dic,url=None,soup=None):
        '''
        :param search_reg:
        :param matched_dic:
        :param url:
        :param soup:
        :return:
        '''
        # import pdb
        # pdb.set_trace()

        if not soup:
            if not url:
                raise ValueError('No url provided')
            soup = self.browser.get_soup(url)
        text = self.soup_util.get_text_from_soup(soup)
        # if len(text) < 200
        matches = []
        for sent in nltk.sent_tokenize(text):
            match_wrds = search_reg.findall(sent)
            if match_wrds:
                matches.append((tuple(match_wrds),sent))
        weight = sum([matched_dic[r'\b'+wrd.lower()+r'\b'] for match_wrds,_ in matches for wrd in match_wrds])
        return matches,weight,text

    def get_res_webpage_base_url(self,base_url):
        '''
        :param base_url:
        :return:
        '''
        base_url = self.url_cleaner.clean_url(base_url,False)
        try:
            soup = self.browser.get_soup(base_url)
        except : #if connection error, try with secure true
            base_url = self.url_cleaner.clean_url(base_url,True)
            soup = self.browser.get_soup(base_url)
        matches,weight,page_all_text = self.search_webpage_single(self.search_reg,self.matched_dic,soup=soup)
        # urls is of form [(url1,text1),(url2,text2),..], emails is a list of emails
        urls, emails = self.soup_util.get_all_links_soupinput(soup,base_url)
        # find if there is a login page
        login_signup_present = True if [1 for url,text in urls if re.search(r'\blogin\b|\bsign(.)?up\b',url,re.IGNORECASE)
                    or re.search(r'\blogin\b|\bsign(.)?up\b|\bsign(.)?in',text,re.IGNORECASE)] else False
        # find if there is a demo page
        demo_present = True if [1 for url,text in urls if re.search(r'\bdemo\b|\btrial\b|\btry( for) free\b',url,re.IGNORECASE)
                    or re.search(r'\bdemo\b|\btrial\b',text,re.IGNORECASE)] else False
        # find if there is a pricing page
        pricing_present = True if [1 for url,text in urls if re.search(r'\bplans\b|\bpricing\b|\bpayment\b|billing',url,re.IGNORECASE)
                    or re.search(r'\bpricing\b|\bpayment\b|billing',text,re.IGNORECASE)] else False
        # find suitable urls and crawl them
        base_url_domain = re.sub('^http(s)?(://)?|www\.','',base_url)
        urls = [(url,text) for url,text in urls if not ((base_url_domain not in url) or re.search('\.png$|zip$',url))]
        matching_urls = [(url,text) for url,text in urls if self.keyword_regex.search(url) or self.keyword_regex.search(text)]
        rest_urls = list(set(urls)-set(matching_urls))
        shuffle(rest_urls)
        urls = matching_urls + rest_urls[:min(len(rest_urls),self.min_pages_per_link)]
        for ind in range(len(urls)):
            url,text = urls[ind]
            logging.info('trying the next level url:{}'.format(url.decode('utf-8','ignore')))
            # get soup object
            soup = self.browser.get_soup(url)
            matches_tmp,weight_tmp,page_text = self.search_webpage_single(self.search_reg,self.matched_dic,soup=soup)
            matches.extend(matches_tmp)
            weight = weight+weight_tmp
            urls_tmp,mails_tmp = self.soup_util.get_all_links_soupinput(soup,base_url)
            emails.extend(mails_tmp)
            urls.extend(urls_tmp)
            page_all_text += ' '+page_text

        urls = list(set([url for url,text in urls if self.social_url_searcher.search(url)]))
        emails = list(set(emails))
        matches = list(set(matches))
        return urls,emails,matches,login_signup_present,weight,demo_present,pricing_present,page_all_text

    def get_res_webpage_base(self,base_url):
        '''
        :param base_url:
        :return:
        '''
        try:
            urls,emails,matches,login_signup_present,weight,demo_present,pricing_present,page_all_text = self.get_res_webpage_base_url(
                base_url=base_url
            )
            logging.info('Extracted info: urls: {} , emails: {} ,'
                         'weight: {}'.format(urls,emails,weight))
            return urls,emails,matches,login_signup_present,weight,demo_present,pricing_present,page_all_text
        except:
            logging.exception('Error happened while trying url: {}'.format(base_url))
            self.browser.exit()
            self.browser.start_browser(visible=self.visible)
            return [],[],[],False,-8888,False,False,' '

    def search_webpage_csv_input(self,websites_loc,out_loc = 'website_extraction_output.xls'):
        '''
        :param websites_loc:
        :param search_wts_loc:
        :param out_loc:
        :return:
        '''
        df = pd.read_csv(websites_loc,sep=None)
        df = df.fillna('')
        websites = list(df[website_column])
        comp_lkdn_urls = list(df[company_linkedin_col])
        row_ids = list(df[id_col])

        out_dic = {'website':[],'score':[],'emails':[],'urls':[],'login_present':[],
                   'demo_present':[],'pricing_present':[],'company_linkedin_url':[],'id':[]}
        # create file for saving all texts from the crawling
        all_texts_file = open(re.sub('\.xls|\.csv','_all_texts.csv',out_loc),'w')
        all_texts_file_writer = csv.writer(all_texts_file)
        ind = 0
        # self.crawler.start_browser(visible=self.visible)
        for website,comp_lkdn_url,row_id in izip(websites,comp_lkdn_urls,row_ids):
            logging.info('Trying for url: {},linkedin_url:{},id:{}'.format(website,comp_lkdn_url,row_id))
            if not website:
                if not comp_lkdn_url or not self.lkdn_parser:
                    website = ''
                else:
                    lkdn_urls = comp_lkdn_url.split(';')
                    lkdn_urls = [i for i in lkdn_urls if i and re.search(r'linkedin.com/company/',i)]
                    if lkdn_urls:
                        comp_lkdn_url = lkdn_urls.pop()
                    if lkdn_urls: #if more than 1 linkedin url, append the rest to the lists
                        for i in lkdn_urls:
                            websites.append(website)
                            row_ids.append(row_id)
                            comp_lkdn_urls.append(i)
                    lkdn_dets = self.lkdn_parser.get_linkedin_details(comp_lkdn_url)
                    if not lkdn_dets.get('Website',''):
                        # try one more time
                        lkdn_dets = self.lkdn_parser.get_linkedin_details(comp_lkdn_url)
                        if not lkdn_dets.get('Website',''):
                            website = ''
                        else:
                            website = lkdn_dets['Website']
                    else:
                        website = lkdn_dets['Website']
            website = self.url_cleaner.clean_url(website,False)
            if url_validation_reg.search(website):
                urls,emails,matches,login_signup_present,weight,demo_present,pricing_present,page_all_text = self.get_res_webpage_base(
                    base_url=website
                )
                if weight == -8888: #-8888 means some error happened
                    continue
                else:
                    logging.info('Extracted info: urls: {} , emails: {} ,'
                                 'weight: {}'.format(urls,emails,weight))
            else:
                logging.info('website failed url validation check. url: {}'.format(website))
                urls,emails,matches,login_signup_present,weight,demo_present,pricing_present,page_all_text = [],[],[],False,-9999,False,False,' '
            out_dic['website'].append(website)
            out_dic['score'].append(weight)
            out_dic['emails'].append(emails)
            out_dic['urls'].append(urls)
            # out_dic['match_texts_test'].append(matches)
            out_dic['login_present'].append(login_signup_present)
            out_dic['demo_present'].append(demo_present)
            out_dic['pricing_present'].append(pricing_present)
            out_dic['id'].append(row_id)
            out_dic['company_linkedin_url'].append(comp_lkdn_url)
            # if ind == 20:
            #     ind = 0
            #     self.browser.exit()
            #     self.browser.start_browser(visible=self.visible)
            #     self.lkdn_parser.exit()
            #     self.lkdn_parser.start(proxy=self.proxy,login=self.login_linkedin,visible=self.visible)
            with open(re.sub('\.xls|\.csv','',out_loc)+'_dic.pkl','w') as f:
                pickle.dump(out_dic,f)
            all_texts_file_writer.writerow([website,page_all_text.encode('utf8')])
        all_texts_file.close()
        out_df = pd.DataFrame(out_dic)
        out_df = out_df.sort_values('score',ascending=False)
        try:
            out_df.to_excel(out_loc,index=False)
        except :
            logging.exception('can not save as excel, try to save as csv')
            try:
                out_df.to_csv(out_loc,index=False)
            except Exception as err:
                logging.exception('can not save as csv, try to save out_dic')
                with open(re.sub('\.xls|\.csv','',out_loc)+'_dic.pkl','w') as f:
                    pickle.dump(out_dic,f)
        self.browser.exit()
        if self.lkdn_parser:
            self.lkdn_parser.exit()


if __name__ == "__main__":
    optparser = OptionParser()
    optparser.add_option('-w', '--websites',
                         dest='website_file',
                         help='location of websites csv file',
                         default=None)
    optparser.add_option('-s', '--searchfile',
                         dest='search_text_file',
                         help='location of csv file with search_terms and weights',
                         default=None)
    optparser.add_option('-o', '--outfile',
                         dest='out_file',
                         help='location of output csv file',
                         default='website_extraction_output.xls')
    optparser.add_option('-v', '--visible',
                         dest='visible',
                         help='if 1 visible, if 0 not visible',
                         default=0,type='int')
    optparser.add_option('-m', '--minpages',
                         dest='minpages',
                         help='no of pages to crawl within a url',
                         default=1,type='int')
    optparser.add_option('-l', '--linkedin_search',
                         dest='linkedin_search',
                         help='use linkedin urls to get details',
                         default=0,type='int')
    (options, args) = optparser.parse_args()
    website_file = options.website_file
    search_text_file = options.search_text_file
    out_file = options.out_file
    visible = options.visible
    min_pages = options.minpages
    linkedin_search = options.linkedin_search
    wpe = ChargebeeWebSearcher(search_text_file,visible,min_pages_per_link=min_pages,search_linkedin=linkedin_search)
    logging.info('started crawling')
    wpe.search_webpage_csv_input(website_file,out_file)
    logging.info('completed crawling')
    wpe.browser.exit()