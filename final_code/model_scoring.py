__author__ = 'joswin'
import logging
import datetime
logging.basicConfig(filename='logs/website_scorer_{}.log'.format(datetime.datetime.now()).replace(' ','_'),
                    level=logging.INFO,format='%(asctime)s %(message)s')
import csv
import pickle
import re
import pandas as pd
from scipy.sparse import hstack
from scipy.sparse.csr import csr_matrix

from optparse import OptionParser

from chargebee_enrichment import ChargebeeWebSearcher


class ScoreWebsite(object):
    '''
    '''
    def __init__(self,search_text_file = 'chargebee_search_texts.csv',visible=False):
        '''
        :return:
        '''
        # logging.basicConfig(filename='logs/website_scorer_{}.log'.format(datetime.datetime.now()), level=logging.INFO,format='%(asctime)s %(message)s')
        with open('chargebee_model_objects_jan5.pkl','r') as f:
            tmp = pickle.load(f)
        self.vectorizer,self.model,self.bin_boundaries,self.bin_labels = tmp['vectorizer'],tmp['model'],tmp['bin_boundaries'],tmp['bin_labels']
        self.reg_text_permonth = re.compile(r'(([0-9]|\$|\b|/)per|/)(\b| |\n|\r){0,2}(month|year|\bmo)\b',re.IGNORECASE)
        self.reg_coming_soon = re.compile(r'coming soon',re.IGNORECASE)
        self.wpe = ChargebeeWebSearcher(search_text_file,visible=visible,min_pages_per_link=2,search_linkedin=True)


    def predict_from_dets(self,text,demo_present,login_present,pricing_present):
        '''
        :param text:
        :param demo_present:
        :param login_present:
        :param pricing_present:
        :return:
        '''
        tfidf = self.vectorizer.transform([text])
        per_month_present = 1 if self.reg_text_permonth.search(text) else 0
        coming_soon = 1 if self.reg_coming_soon.search(text) else 0
        tfidf_n_vars = hstack([tfidf,
                               csr_matrix([demo_present,login_present,pricing_present,per_month_present,coming_soon])])
        score = self.model.predict_proba(tfidf_n_vars)[0,1]
        bin_number = list(pd.cut(pd.Series(score),self.bin_boundaries,labels=self.bin_labels))[0]
        return score,bin_number

    def get_score_website(self,website,linkedin_url=None,additional_data=True):
        '''
        :param website:
        :return:
        '''
        if not website:
            if linkedin_url and self.wpe.lkdn_parser:
                lkdn_dets = self.wpe.lkdn_parser.get_linkedin_details(linkedin_url)
                website = lkdn_dets.get('Website')
        if website:        
            urls,emails,matches,login_signup_present,weight,demo_present,pricing_present,page_all_text = self.wpe.get_res_webpage_base(website)
            score,bin_number = self.predict_from_dets(page_all_text,demo_present,login_signup_present,pricing_present)
        else:
            urls,emails,matches,login_signup_present,weight,demo_present,pricing_present,page_all_text = [],[],[],False,-8888,False,False,' '
            score,bin_number = 0,0
        if additional_data:
            return score,bin_number,urls,emails,matches,login_signup_present,weight,demo_present,pricing_present,page_all_text
        else:
            return score,bin_number

    def exit(self):
        '''
        :return:
        '''
        self.wpe.close()

class ScoreWebsiteCSV(object):
    def __init__(self,visible=False):
        # logging.basicConfig(filename='logs/website_scorer_csv_files_{}.log'.format(datetime.datetime.now()),
        #                     level=logging.INFO,format='%(asctime)s %(message)s')
        self.scorer_obj = ScoreWebsite(visible=visible)

    def start_scoring(self,inp_loc,out_loc,list_name='sample_list'):
        logging.info('started website scoring from csv file')
        if out_loc[-1] == '/':
            out_loc = out_loc[:-1]

        inp_file = open(inp_loc,'r')
        inp_file_reader = csv.reader(inp_file)
        all_texts_file = open(out_loc+'/'+list_name+'_website_text_file.csv','wb')
        all_texts_file_writer = csv.writer(all_texts_file,quotechar = '"')
        out_loc_file = open(out_loc+'/'+list_name+'_website_score_file.csv','wb')
        out_loc_file_writer = csv.writer(out_loc_file,quotechar = '"')
        #write headers
        all_texts_file_writer.writerow(['website','page_all_text'])
        out_loc_file_writer.writerow(['website','linkedin_url','score','bin_number','weight','login_signup_present','demo_present',
            'pricing_present','urls','emails','matches'])

        row = inp_file_reader.next() #ignore first row
        for row in inp_file_reader:
            logging.info('trying for row:{}'.format(row))
            website = row[0]
            linkedin_url = row[1]
            score,bin_number,urls,emails,matches,login_signup_present,weight,demo_present,pricing_present,page_all_text = \
                self.scorer_obj.get_score_website(website,linkedin_url,additional_data=True)
            all_texts_file_writer.writerow([website,page_all_text.encode('utf8')])
            out_loc_file_writer.writerow([website,linkedin_url,score,bin_number,weight,login_signup_present,demo_present,pricing_present,urls,emails,matches])
            logging.info('completed for row:{},score:{},bin:{}'.format(row,score,bin_number))
        inp_file.close()
        all_texts_file.close()
        out_loc_file.close()
        self.scorer_obj.exit()

if __name__ == "__main__":
    logging.info('started scoring for csv file')
    optparser = OptionParser()
    optparser.add_option('-w', '--website_file',
                         dest='website_file',
                         help='location of websites csv file',
                         default=None)
    optparser.add_option('-o', '--out_loc',
                         dest='out_loc',
                         help='location of output csv files',
                         default=None)
    optparser.add_option('-l', '--list_name',
                         dest='list_name',
                         help='sample list name to be appened on out file name',
                         default='sample_list')
    optparser.add_option('-v', '--visible',
                         dest='visible',
                         help='visible',
                         default=0,type='int')
    (options, args) = optparser.parse_args()
    website_file = options.website_file
    out_loc = options.out_loc
    list_name = options.list_name
    visible = options.visible
    csv_scorer = ScoreWebsiteCSV(visible=visible)
    csv_scorer.start_scoring(inp_loc=website_file,out_loc=out_loc,list_name=list_name)