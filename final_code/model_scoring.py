__author__ = 'joswin'

import pickle
import re
import pandas as pd
from scipy.sparse import hstack
from scipy.sparse.csr import csr_matrix

from chargebee_enrichment import ChargebeeWebSearcher

class ScoreWebsite(object):
    '''
    '''
    def __init__(self,search_text_file = 'chargebee_search_texts.csv'):
        '''
        :return:
        '''
        with open('chargebee_model_objects_jan5.pkl','r') as f:
            tmp = pickle.load(f)
        self.vectorizer,self.model,self.bin_boundaries,self.bin_labels = tmp['vectorizer'],tmp['model'],tmp['bin_boundaries'],tmp['bin_labels']
        self.reg_text_permonth = re.compile(r'(([0-9]|\$|\b|/)per|/)(\b| |\n|\r){0,2}(month|year|\bmo)\b',re.IGNORECASE)
        self.reg_coming_soon = re.compile(r'coming soon',re.IGNORECASE)
        self.wpe = ChargebeeWebSearcher(search_text_file,visible=False,min_pages_per_link=2,search_linkedin=True)


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

    def get_score_website(self,website):
        '''
        :param website:
        :return:
        '''
        urls,emails,matches,login_signup_present,weight,demo_present,pricing_present,page_all_text = self.wpe.get_res_webpage_base(website)
        score,bin_number = self.predict_from_dets(page_all_text,demo_present,login_signup_present,pricing_present)
        return score,bin_number

    def exit(self):
        '''
        :return:
        '''
        self.wpe.close()