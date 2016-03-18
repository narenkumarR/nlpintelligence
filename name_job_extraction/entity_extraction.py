import sys
import pdb
import json
import re

# from corenlp import StanfordCoreNLP
import nltk
from nltk.tag.stanford import StanfordNERTagger

class StanfordNERTaggerExtractor(object):
    """docstring for ClassName"""
    def __init__(self,three_class_jar_loc='stanford-jars/english.all.3class.distsim.crf.ser.gz',\
                 ner_jar_loc = 'stanford-jars/stanford-ner.jar' ):
        '''
        :param three_class_jar_loc: 3 class jar file
        :param ner_jar_loc: ner jar file
        :return:
        '''
        self.st = StanfordNERTagger(three_class_jar_loc ,
            ner_jar_loc)

    def tag_text_single(self,text):
        '''
        :param text:
        :return:
        '''
        # assert type(text) == str
        sents = self.st.tag(nltk.word_tokenize(text))
        return sents

    def identify_NER_tags_single(self,text_tag,tag_to_find):
        '''
        :param text_tag: Tagged text
        :param tag_to_find:
        :return:
        '''
        tag_strs = []
        prev_wrd_tag = False
        for wrd,tag in text_tag:
            if tag == tag_to_find:
                if not prev_wrd_tag:
                    tag_strs.append(wrd)
                else:
                    prev_wrd = tag_strs.pop()
                    new_wrd = prev_wrd+' '+wrd
                    tag_strs.append(new_wrd)
                prev_wrd_tag = True
            else:
                prev_wrd_tag = False
        tags_final = []
        for wrd in tag_strs:
            if wrd not in tags_final:
                tags_final.append(wrd)
        return tags_final

    def tag_text_multi(self,text):
        ''' '''
        tokenized_sents = [nltk.word_tokenize(sent) for sent in nltk.sent_tokenize(text)]
        return self.st.tag_sents(tokenized_sents)

    def identify_NER_tags_multi(self,text_tag,tag_to_find):
        tag_strs = []
        for sent_tag in text_tag:
            for wrd in self.identify_NER_tags_single(sent_tag,tag_to_find):
                if wrd not in tag_strs:
                    tag_strs.append(wrd)
        return tag_strs


class JobsExtractor(object):
    def __init__(self,job_json='job_designations_all.json'):
        with open(job_json,'r') as f:
            jobs_dict = json.load(f)
        top_jobs_regex = r'\b|\b'.join(jobs_dict['top_jobs'])
        top_jobs_regex = r'\b'+top_jobs_regex+r'\b'
        self.reg_top_jobs = re.compile(top_jobs_regex,re.IGNORECASE)
        # self.reg_jobs = re.compile('|'.join(jobs_dict['jobs']),re.IGNORECASE)
        naukri_jobs = jobs_dict['jobs']
        jobs_regex = r'\b|\b'.join(naukri_jobs)
        jobs_regex = r'\b'+jobs_regex+r'\b'
        self.reg_jobs = re.compile(jobs_regex)

    def find_top_jobs(self,text):
        ''' '''
        jobs = []
        for match in self.reg_top_jobs.finditer(text):
            jobs.append(text[match.start():match.end()])
        return jobs

    def find_jobs(self,text):
        ''' '''
        jobs = []
        for match in self.reg_jobs.finditer(text):
            jobs.append(text[match.start():match.end()])
        return jobs

    def find_all_jobs(self,text):
        ''' '''
        jobs = []
        for i in self.find_top_jobs(text):
            if i not in jobs:
                jobs.append(i)
        for i in self.find_jobs(text):
            if i not in jobs:
                jobs.append(i)
        # jobs = list(set(jobs))
        return jobs
