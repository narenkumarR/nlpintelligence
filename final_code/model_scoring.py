__author__ = 'joswin'
import logging
import datetime
import json
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
from postgres_connect import PostgresConnect


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
            urls,emails,matches,login_signup_present,weight,demo_present,pricing_present,page_all_text,base_url_source = self.wpe.get_res_webpage_base(website)
            score,bin_number = self.predict_from_dets(page_all_text,demo_present,login_signup_present,pricing_present)
        else:
            urls,emails,matches,login_signup_present,weight,demo_present,pricing_present,page_all_text,base_url_source = [],[],[],False,-8888,False,False,' ',''
            score,bin_number = 0,0
        if additional_data:
            return score,bin_number,urls,emails,matches,login_signup_present,weight,demo_present,pricing_present,page_all_text,base_url_source
        else:
            return score,bin_number

    def exit(self):
        '''
        :return:
        '''
        self.wpe.close()

class ScoreWebsiteCSV(object):
    def __init__(self,visible=False,search_text_file = 'chargebee_search_texts.csv',scorer_name='saas_score'):
        # logging.basicConfig(filename='logs/website_scorer_csv_files_{}.log'.format(datetime.datetime.now()),
        #                     level=logging.INFO,format='%(asctime)s %(message)s')
        self.scorer_obj = ScoreWebsite(search_text_file=search_text_file,visible=visible)
        self.scorer_name = scorer_name
        self.con = PostgresConnect()

    def start_scoring(self,inp_loc,out_loc,list_name='sample_list'):
        logging.info('started website scoring from csv file')
        if out_loc[-1] == '/':
            out_loc = out_loc[:-1]

        inp_file = open(inp_loc,'r')
        inp_file_reader = csv.reader(inp_file)
        #all_texts_file = open(out_loc+'/'+list_name+'_website_text_file.csv','wb')
        #all_texts_file_writer = csv.writer(all_texts_file,quotechar = '"')
        out_loc_file = open(out_loc+'/'+list_name+'_website_score_file.csv','wb')
        out_loc_file_writer = csv.writer(out_loc_file,quotechar = '"')
        #write headers
        #all_texts_file_writer.writerow(['website','page_all_text'])
        out_loc_file_writer.writerow(['website','linkedin_url','score','bin_number','weight','login_signup_present','demo_present',
            'pricing_present','urls','emails','matches'])

        row = inp_file_reader.next() #ignore first row
        for row in inp_file_reader:
            logging.info('trying for row:{}'.format(row))
            website = row[0]
            linkedin_url = row[1]
            score,bin_number,urls,emails,matches,login_signup_present,weight,demo_present,pricing_present,page_all_text,base_url_source = \
                self.scorer_obj.get_score_website(website,linkedin_url,additional_data=True)
            # all_texts_file_writer.writerow([website,re.sub(' +',' ',re.sub('[^A-Za-z0-9?!. ]+', ' ', page_all_text)).encode('utf8')])
            out_loc_file_writer.writerow([website,linkedin_url,score,bin_number,weight,login_signup_present,demo_present,pricing_present,urls,emails,matches])
            self.save_to_table(website,None,page_all_text,base_url_source,urls=urls,emails=emails,
                login_signup_present=login_signup_present,demo_present=demo_present,pricing_present=pricing_present,score=weight)
            logging.info('completed for row:{},score:{},bin:{}'.format(row,score,bin_number))
        inp_file.close()
        #all_texts_file.close()
        out_loc_file.close()
        self.scorer_obj.exit()

    def save_to_table(self,base_url,base_url_text,rest_url_text,base_url_source='',**kwargs):
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
        score = kwargs.pop('score')
        kwargs[self.scorer_name] = score
        self.con.get_cursor()
        query = " insert into webpage_texts (url,all_page_text,home_page_text,home_page_source,misc_details) values " \
                " (%s,%s,%s,%s,%s)"
        self.con.cursor.execute(query,(base_url,rest_url_text,base_url_text,base_url_source,json.dumps(kwargs)))
        self.con.commit()
        self.con.close_cursor()

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
    optparser.add_option( '--search_text_file',
                         dest='search_text_file',
                         help='search_text_file location',
                         default='chargebee_search_texts.csv')
    optparser.add_option( '--scorer_name',
                         dest='scorer_name',
                         help='scorer_name to be saved into table',
                         default='saas_score')
    optparser.add_option('-v', '--visible',
                         dest='visible',
                         help='visible',
                         default=0,type='int')
    (options, args) = optparser.parse_args()
    website_file = options.website_file
    out_loc = options.out_loc
    list_name = options.list_name
    visible = options.visible
    search_text_file = options.search_text_file
    scorer_name = options.scorer_name
    csv_scorer = ScoreWebsiteCSV(visible=visible,search_text_file=search_text_file,scorer_name=scorer_name)
    csv_scorer.start_scoring(inp_loc=website_file,out_loc=out_loc,list_name=list_name)