__author__ = 'joswin'

import logging
import nltk
import pandas as pd
import re
from itertools import izip
from optparse import OptionParser
from random import shuffle

from selenium_crawl import SeleniumParser
from utils import SoupUtils
from constants import website_column,search_text_column,search_text_weight_column
from url_cleaner import UrlCleaner

logging.basicConfig(filename='link_finder.log', level=logging.INFO,format='%(asctime)s %(message)s')

class LinkFinderSingle(object):

    def __init__(self,visible=True):
        self.soup_util = SoupUtils()
        self.visible = visible
        self.crawler = SeleniumParser(visible=visible)
        self.url_cleaner = UrlCleaner()

    def get_all_links(self,base_url,depth=1,max_url_in_depth=20):
        ''' get all links
        :param url:
        :param depth: not implemented?
        :return:
        '''
        url = self.url_cleaner.clean_url(base_url)
        base_url_cleaned = re.sub('http(s)?://www.','',base_url)
        soup = self.crawler.get_soup(url)
        urls_texts,mails = self.soup_util.get_all_links_soupinput(soup,url)
        if urls_texts:
            all_urls = [i[0] for i in urls_texts]
        else:
            all_urls = []
        if depth == 0 or not all_urls:
            return all_urls,urls_texts,mails
        all_urls_texts = urls_texts
        all_mails = mails
        urls_crawled = [url]
        for _ in range(depth):
            urls_to_crawl = list(set(all_urls) - set(urls_crawled))
            urls_to_crawl = [i for i in urls_to_crawl if base_url_cleaned in i]
            urls_to_crawl = urls_to_crawl[:max_url_in_depth]
            urls_level,mails_level,urls_texts_level = [],[],[]
            for url in urls_to_crawl:
                soup = self.crawler.get_soup(url)
                urls,mails = self.soup_util.get_all_links_soupinput(soup,url)
                # import pdb
                # pdb.set_trace()
                if urls:
                    urls_texts_level.extend(urls)
                    urls_level.extend([i[0] for i in urls])
                mails_level.append(mails)
                urls_crawled.append(url)
            # all_urls_texts.extend(urls_level)
            all_mails.extend(mails_level)
            if urls_level:
                all_urls_texts.extend(urls_texts_level)
                all_urls.extend(urls_level)
        return all_urls,all_urls_texts,all_mails

    def get_matching_links(self,base_url,matcher_exp,depth=1,max_url_in_depth=20):
        '''
        :param base_url:
        :param matcher_exp:
        :param depth:
        :param max_url_in_depth:
        :return:
        '''
        match_regex = re.compile(matcher_exp,re.IGNORECASE)
        all_urls,_,_ = self.get_all_links(base_url,depth,max_url_in_depth)
        matched_urls = [url for url in all_urls if (type(url) == str or type(url)==unicode) and match_regex.search(url)]
        return list(set(matched_urls))