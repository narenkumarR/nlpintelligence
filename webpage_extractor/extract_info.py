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

logging.basicConfig(filename='website_extraction.log', level=logging.INFO,format='%(asctime)s %(message)s')

class WebPageTextSearch(object):
    '''
    '''
    def __init__(self,visible=True):
        '''
        :return:
        '''
        self.soup_util = SoupUtils()
        self.visible = visible
        self.crawler = SeleniumParser(visible=self.visible)
        self.url_cleaner = UrlCleaner()

    def search_webpage_single(self,search_texts,search_text_weights=None,url=None,soup=None):
        '''
        :param url:
        :param search_texts: list of texts to be matched
        :param search_text_weights: list of weights
        :return:
        '''
        # import pdb
        # pdb.set_trace()
        matched_dic = {}
        if search_text_weights:
            for text,weight in izip(search_texts,search_text_weights):
                matched_dic[text.lower()] = weight
        else:
            for text in search_texts:
                matched_dic[text.text.lower()] = 1
        if not soup:
            if not url:
                raise ValueError('No url provided')
            soup = self.crawler.get_soup(url)
        text = self.soup_util.get_text_from_soup(soup)
        # search_reg_text = '\y' + '\y|\y'.join(search_texts) + '\y'
        search_reg_text = '|'.join(search_texts)
        search_reg = re.compile(search_reg_text,re.IGNORECASE)
        matches = []
        for sent in nltk.sent_tokenize(text):
            match_wrds = search_reg.findall(sent)
            if match_wrds:
                matches.append((tuple(match_wrds),sent))
        weight = sum([matched_dic[wrd.lower()] for match_wrds,_ in matches for wrd in match_wrds])
        return matches,weight

    def search_webpage_base(self,base_url,search_texts,search_text_weights=None):
        '''
        :param base_url:
        :param search_texts:
        :param search_text_weights:
        :return:
        '''
        base_url = self.url_cleaner.clean_url(base_url,False)
        soup = self.crawler.get_soup(base_url)
        matches,weight = self.search_webpage_single(search_texts,search_text_weights,soup=soup)
        urls, emails = self.soup_util.get_all_links_soupinput(soup,base_url)
        urls = [(url,text) for url,text in urls if not ((not base_url in url) or re.search('\.png$',url) or 'login' in url)]
        shuffle(urls)
        for ind in range(min(len(urls),10)):
            url,text = urls[ind]
            if (not base_url in url) or re.search('\.png$',url) or 'login' in url:
                continue
            soup = self.crawler.get_soup(url)
            matches_tmp,weight_tmp = self.search_webpage_single(search_texts,search_text_weights,soup=soup)
            matches.extend(matches_tmp)
            weight = weight+weight_tmp
            urls_tmp,mails_tmp = self.soup_util.get_all_links_soupinput(soup,base_url)
            emails.extend(mails_tmp)
            urls.extend(urls_tmp)
        out_url_searcher = re.compile(r'linkedin|facebook',re.IGNORECASE)
        urls = list(set([url for url,text in urls if out_url_searcher.search(url)]))
        emails = list(set(emails))
        matches = list(set(matches))
        return urls,emails,matches,weight

    def search_webpage_csv_input(self,websites_loc,search_wts_loc,out_loc = 'website_extraction_output.xls'):
        '''
        :param websites_loc:
        :param search_wts_loc:
        :param out_loc:
        :return:
        '''
        websites = list(pd.read_csv(websites_loc)[website_column])
        search_texts_df = pd.read_csv(search_wts_loc)
        search_wrds_list = list(search_texts_df[search_text_column])
        search_wrds_weights_list = list(search_texts_df[search_text_weight_column])
        out_dic = {'website':[],'score':[],'emails':[],'urls':[],'match_texts_test':[]}
        ind = 0
        self.crawler.start_browser(visible=self.visible)
        for website in websites:
            logging.info('Trying for url : {}'.format(website))
            try:
                urls,emails,matches,weight = self.search_webpage_base(website,search_wrds_list,search_wrds_weights_list)
                logging.info('Extracted info: urls: {} , emails: {} ,matches : {} ,weight: {}'.format(urls,emails,matches,weight))
            except:
                logging.exception('Error happened while trying url: {}'.format(website))
                self.crawler.exit()
                self.crawler.start_browser(visible=self.visible)
                continue
            out_dic['website'].append(website)
            out_dic['score'].append(weight)
            out_dic['emails'].append(emails)
            out_dic['urls'].append(urls)
            out_dic['match_texts_test'].append(matches)
            if ind == 20:
                ind = 0
                self.crawler.exit()
                self.crawler.start_browser(visible=self.visible)
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
                with open('dic_'+out_loc,'w') as f:
                    import pickle
                    pickle.dump(out_dic,f)


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
    (options, args) = optparser.parse_args()
    website_file = options.website_file
    search_text_file = options.search_text_file
    out_file = options.out_file
    visible = options.visible

    wpe = WebPageExtractor(visible)
    wpe.search_webpage_csv_input(website_file,search_text_file,out_file)
    wpe.crawler.exit()
