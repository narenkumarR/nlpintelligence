__author__ = 'joswin'

import logging
import nltk
import pandas as pd
import re
from itertools import izip
from optparse import OptionParser
from random import shuffle
import tldextract
from collections import Counter

from selenium_crawl import SeleniumParser
from utils import SoupUtils
from constants import website_column
from url_cleaner import UrlCleaner
import pickle

logging.basicConfig(filename='job_links_finding.log', level=logging.INFO,format='%(asctime)s %(message)s')

class JobPortalSearch(object):
    '''
    '''
    def __init__(self,visible=True,min_pages_per_link=10):
        '''
        :return:
        '''
        self.soup_util = SoupUtils()
        self.visible = visible
        self.crawler = SeleniumParser(visible=self.visible)
        self.url_cleaner = UrlCleaner()
        self.career_link_regex = re.compile(r'careers|jobs',re.IGNORECASE)
        self.career_linktext_regex = re.compile(r'careers|jobs',re.IGNORECASE)
        self.min_pages_per_link = min_pages_per_link
        self.social_media_regex = re.compile(r'linkedin|facebook|twitter|plus\.google\.com',re.IGNORECASE)
        self.career_link_domain_set = {'careers','jobs','mobile','blog'} #used for domain checking in search_webpage_base
        self.jobs_regex = re.compile(r'developer|executive|manager|accountant|associate|'
                                     r'engineer|programmer|scientist|\blead\b|security|software|senior|'
                                     r'intern|designer|research|compliance|producer|analyst|director|'
                                     r'architect|writer|researcher|marketer',re.IGNORECASE)

    def get_career_page(self,soup,base_url):
        '''
        :param soup:
        :return:
        '''
        urls, emails = self.soup_util.get_all_links_soupinput(soup,base_url,True)
        return list(set([re.sub('/$','',url) for url,text in urls
                 if self.career_link_regex.search(url) or self.career_linktext_regex.search(text)]))

    def get_contact_dets(self,soup,base_url):
        '''
        :param soup:
        :param base_url:
        :return:social_medial_links_list,emails_list
        '''
        urls, emails = self.soup_util.get_all_links_soupinput(soup,base_url)
        return [url for url,_ in urls if self.social_media_regex.search(url)],emails

    def get_jobpost_dets(self,soup,base_url,url):
        ''' checks all job postings and see if they are linked to a different site. In that case, we can assume
        that the website has a tie up with a job portal like site.
        :param soup:
        :param base_url:
        :return:flag 1: job portal url present. 0: not present
        '''
        portal_present,upload_resume,email_only = False,False,False
        emails = []



    def search_career_page(self,base_url,career_url):
        '''
        :param base_url:
        :param career_url:
        :return:
        '''
        portal_present,upload_resume,email_present = False,False,False
        emails = []
        ext_base = tldextract.extract(base_url)
        if ext_base.subdomain:
            base_doms = {ext_base.domain,ext_base.subdomain}
        else:
            base_doms = {ext_base.domain}
        ext_url = tldextract.extract(career_url)
        if ext_url.subdomain:
            url_doms = {ext_url.domain,ext_url.subdomain}
        else:
            url_doms = {ext_url.domain}
        # checking if the career page has a different domain
        if not url_doms.intersection(base_doms): #if
            logging.info('career url has completely different domain than base url')
            portal_present = True
        else:
            # checking if domain or subdomain is different in the career page
            url_only = url_doms - base_doms
            if url_only and not url_only.intersection(self.career_link_domain_set):
                portal_present = True
                logging.info('career url has somewhat different domain/subdomain than base url')
        if not portal_present:
            soup = self.crawler.get_soup(career_url)
            if re.search('upload resume',soup.text,re.IGNORECASE):
                upload_resume = True
            urls, emails = self.soup_util.get_all_links_soupinput(soup,base_url)
            job_postings = [url for url,text in urls if self.jobs_regex.search(text)]
            # for url in job_postings:
            #     ext_url = tldextract.extract(url)
            #     if ext_url.subdomain:
            #         url_doms = {ext_url.domain,ext_url.subdomain}
            #     else:
            #         url_doms = {ext_url.domain}
            #     if not url_doms.intersection(base_doms): #if
            #         portal_present = True
            #         break
            #     else:
            #         # checking if domain or subdomain is different in the career page
            #         url_only = url_doms - base_doms
            #         if url_only and not url_only.intersection(self.career_link_domain_set):
            #             import pdb
            #             pdb.set_trace()
            #             portal_present = True
            #             break
            domains = []
            for url in job_postings:
                ext_url = tldextract.extract(url)
                domains.append(ext_url.domain)
                if ext_url.subdomain and ext_url.subdomain != 'www':
                    domains.append(ext_url.subdomain)
            dom_cnts = Counter(domains)
            common_doms = dom_cnts.most_common(3)
            base_cnt = dom_cnts[ext_base.domain]
            for val,cnt in common_doms:
                if val != ext_base.domain:
                    if cnt > 0.7*base_cnt:
                        portal_present = True
                        break
        if emails:
            email_present = True
        return portal_present,upload_resume,email_present,emails

    def search_webpage_base(self,base_url_orig):
        '''
        :param base_url:
        :param search_texts:
        :param search_text_weights:
        :return:
        '''
        base_url = self.url_cleaner.clean_url(base_url_orig,False)
        # parsed_uri = urlparse(base_url)
        soup = self.crawler.get_soup(base_url)
        if soup.title.text == 'Cyberoam Captive Portal':
            # return as error happened. need to process again later
            return False,False,False,[],True
        career_urls = self.get_career_page(soup,base_url)
        # social_urls,emails = self.get_contact_dets(soup,base_url)
        shuffle(career_urls)
        if not career_urls:
            logging.info('could not find career url for url: {}'.format(base_url))
            portal_present,upload_resume,email_present = False,False,False
            emails = []
        else:
            portal_present,upload_resume,email_present,emails = self.search_career_page(base_url,career_urls[0])
        return portal_present,upload_resume,email_present,emails,False

    def search_webpage_csv_input(self,websites_loc,out_loc = 'website_extraction_output.xls'):
        '''
        :param websites_loc:
        :param out_loc:
        :return:
        '''
        websites = list(pd.read_csv(websites_loc)[website_column])
        out_dic = {'website':[],'portal_present':[],'upload_resume':[],'email_present':[],'emails':[]}
        ind = 0
        # self.crawler.start_browser(visible=self.visible)
        for website in websites:
            logging.info('Trying for url : {}'.format(website))
            try:
                portal_present,upload_resume,email_present,emails,error_dt = self.search_webpage_base(website)
            except:
                logging.exception('Error happened while trying url: {}'.format(website))
                self.crawler.exit()
                self.crawler.start_browser(visible=self.visible)
                continue
            if error_dt:
                logging.info('blocked by admin for url: {}'.format(website))
                continue
            logging.info('Extracted info:portal_present: {},upload_resume: {},email_present: {},'
                             'emails: {}'.format(portal_present,upload_resume,email_present,emails))
            out_dic['website'].append(website)
            out_dic['emails'].append(emails)
            out_dic['portal_present'].append(portal_present)
            out_dic['email_present'].append(email_present)
            out_dic['upload_resume'].append(upload_resume)
            with open(re.sub('\.xls|\.csv','',out_loc)+'_dic.pkl','w') as f:
                pickle.dump(out_dic,f)
            # if ind == 20:
            #     ind = 0
            #     self.crawler.exit()
            #     self.crawler.start_browser(visible=self.visible)
        out_df = pd.DataFrame(out_dic)
        try:
            out_df.to_excel(out_loc,index=False)
        except :
            logging.exception('can not save as excel, try to save as csv')
            try:
                out_df.to_csv(out_loc,index=False)
            except Exception as err:
                logging.exception('can not save as csv, try to save out_dic')
                with open('dic_'+out_loc,'w') as f:
                    pickle.dump(out_dic,f)

if __name__ == "__main__":
    optparser = OptionParser()
    optparser.add_option('-w', '--websites',
                         dest='website_file',
                         help='location of websites csv file',
                         default=None)
    optparser.add_option('-o', '--outfile',
                         dest='out_file',
                         help='location of output csv file',
                         default='job_page_search.xls')
    optparser.add_option('-v', '--visible',
                         dest='visible',
                         help='if 1 visible, if 0 not visible',
                         default=0,type='int')
    optparser.add_option('-m', '--minpages',
                         dest='minpages',
                         help='no of pages to crawl within a url',
                         default=10,type='int')
    (options, args) = optparser.parse_args()
    website_file = options.website_file
    out_file = options.out_file
    visible = options.visible
    min_pages = options.minpages
    jps = JobPortalSearch(visible,min_pages_per_link=min_pages)
    jps.search_webpage_csv_input(website_file,out_file)
    jps.crawler.exit()
