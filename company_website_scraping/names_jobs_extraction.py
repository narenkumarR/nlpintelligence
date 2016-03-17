__author__ = 'joswin'
''' Need to fix the import mechanisms. currently need to come to this folder to run this code. if it changes
    the imports should reflect everywhere. ideally assume all codes are run from the main folder'''

import sys
import re
import pdb

import company_link_crawl
import utils

sys.path.append('../')
import name_job_extraction.entity_extraction as ee
import name_job_extraction.matcher_funs as mf

class CompanyNameJobExtractor(object):
    '''
    '''
    def __init__(self,stanford_3class_loc = '../stanford-jars/english.all.3class.distsim.crf.ser.gz'
                    ,stanford_ner_jar_loc = '../stanford-jars/stanford-ner.jar'
                    ,jobs_json_loc = '../name_job_extraction/job_designations_all.json'

    ):
        '''
        :return:
        '''
        self.company_link_crawler = company_link_crawl.CompanyLinkCrawler()
        self.se = ee.StanfordNERTaggerExtractor(stanford_3class_loc,stanford_ner_jar_loc)
        self.matcher = mf.Matcher
        self.je = ee.JobsExtractor(jobs_json_loc)
        self.clc = company_link_crawl.CompanyLinkCrawler()

    def find_jobs_names_soupinput(self,soup,names_remove=[]):
        '''
        :param soup:
        :return:
        '''
        text = self.clc.get_pagetext_soupinput(soup)
        text = re.sub('\n','. ',text)
        text_tagged = self.se.tag_text_multi(text)
        names = self.se.identify_NER_tags_multi(text_tagged,'PERSON')
        if names_remove:
            for name in names:
                for name_remove in names_remove:
                    if utils.match_chars_hard(name,name_remove):
                        names.remove(name)
                        break
        jobs = self.je.find_all_jobs(text)
        names_jobs = self.matcher.match_names_and_jobs(text,names,jobs)
        return names_jobs

    def find_jobs_names_urlinput(self,url,names_remove=[]):
        '''
        :param url:
        :return:
        '''
        pdb.set_trace()
        soup = self.clc.soup_generator.single_wp(url)
        return self.find_jobs_names_soupinput(soup,names_remove)
