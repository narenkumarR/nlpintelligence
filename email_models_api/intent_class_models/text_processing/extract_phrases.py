#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'joswin'

import nltk,re

def tree_travel(tree_obj,stopwords=[]):
        ''' Travel through a tree object
        :param tree_obj:
        :param stopwords:
        :return:
        '''
        tree_data = []
        for e in list(tree_obj):
            if isinstance(e,nltk.tree.Tree):
                # try:
                    tree_data.extend(tree_travel(e,stopwords))
                # except:
                #     continue
            else:
                if e[0] not in stopwords:
                    tree_data+=[e[0]]
        return tree_data

class PhraseExtractor(object):
    def __init__(self):
        pass

    def extract_phrase_treeinput(self,tr,labels,stopwords=[]):
        '''
        :param tr: tree object
        :param labels: list of label to extract phrase
        :param stopwords: stopwords list
        :return:
        '''
        temp_list=[]
        for subtree in tr.subtrees():
            if subtree.label() in labels:
                tree_data = tree_travel(subtree,stopwords)
                temp_list+=[' '.join((e for e in list(tree_data)))]
        return temp_list

    def extract_phrase_treelistinput(self,ltrees,labels,stopwords=[]):
        '''
        :param ltrees: list of tree objects
        :param label: list of labels to extract phrase
        :param stopwords:
        :return:
        '''
        outlist = []
        for tr in ltrees:
            outlist.append(self.extract_phrase_treeinput(tr,labels,stopwords))
        return outlist

class PhraseRemover(object):
    """docstring for PhraseRemover"""
    def __init__(self):
        pass
    
    def remove_phrase_treeinput(self,tr,labels=None,stopwords=[]):
        '''
        :param tr: tree object
        :param labels: list of label to extract phrase
        :param stopwords: stopwords list
        :return:
        '''
        if not labels:
            labels = ['ORGANIZATION','PERSON','LOCATION','DATE','TIME','MONEY','PERCENT','FACILITY','GPE']
        labels.append('S')
        return ' '.join([' '.join(tree_travel(e,stopwords)) if isinstance(e,nltk.tree.Tree) else e[0] 
                    for e in list(tr) if not (isinstance(e,nltk.tree.Tree) and e.label() in labels)])

    def remove_phrase_treelistinput(self,ltrees,labels=None,stopwords=[]):
        '''
        :param ltrees: list of tree objects
        :param label: list of labels to extract phrase
        :param stopwords:
        :return:
        '''
        if not labels:
            labels = ['ORGANIZATION','PERSON','LOCATION','DATE','TIME','MONEY','PERCENT','FACILITY','GPE']
        return [self.remove_phrase_treeinput(tr,labels,stopwords) for tr in ltrees]



def multiple_replace(dict, text, word_limit = False, flags = 0):
    '''replaces multiple matches 
    :param flags: pass re.IGNORECASE etc here
    '''
    # Create a regular expression  from the dictionary keys
    if word_limit:
        reg_text = "(\\b%s\\b)" % "|".join(map(re.escape, dict.keys()))
        reg_text = '\\b'+reg_text+'\\b'
        regex = re.compile(reg_text,flags)
    else:
        regex = re.compile("(%s)" % "|".join(map(re.escape, dict.keys())),flags)
    # For each match, look-up corresponding value in dictionary
    return regex.sub(lambda mo: dict[mo.string[mo.start():mo.end()]], text)

class PhraseMerger(object):
    """merge phrases"""
    def __init__(self):
        pass

    def merge_phrases_textinput_matchdict(self,text,phr_match_dict,word_limit=True,flags=0,join_by='_',keep_original=True):
        '''
        :param text : text input
        :param phr_match_dict : dict which gives matching text and replace text
            eg: {"not interested":"not_interested","no need":"no_need"}
        '''        
        if keep_original:
            new_text = multiple_replace(phr_match_dict,text,word_limit,flags)
            new_text_bck = new_text
            for wrd in nltk.word_tokenize(new_text_bck):
                if join_by in wrd:
                    for wrd1 in wrd.split(join_by):
                        new_text = new_text + ' '+wrd1
        else:
            new_text = multiple_replace(phr_match_dict,text,word_limit,flags)
        return new_text

    def build_phr_match_dict(self,phrases,join_by='_'):
        '''
        build phrases dict from phrases list
        '''
        phr_dict = {}
        for phr in phrases:
            new_phr = join_by.join(nltk.word_tokenize(phr))
            phr_dict[phr] = new_phr
        return phr_dict

    def merge_phrases_textinput(self,text,phrases,join_by = '_',word_limit=True,flag=0,keep_original=False):
        '''
        :param text:
        :param phrases : list of phrases. eg: ['not interested','no need',..]
        :param join_by : how to join phrases
        '''
        return self.merge_phrases_textinput_matchdict(text,self.build_phr_match_dict(phrases,join_by),word_limit,flags,join_by,keep_original)

    def merge_phrases_listinput(self,text_list,phrases,join_by='_',word_limit=True,flags=0,keep_original=False):
        '''
        :param text_list:
        :param phrases:
        :param join_by:
        '''
        new_list = []
        phr_dict = self.build_phr_match_dict(phrases,join_by)
        for text in text_list:
            new_list.append(self.merge_phrases_textinput_matchdict(text,phr_dict,word_limit,flags,join_by,keep_original))
        return new_list

    def merge_word_textinput(self,text,wrd_list,with_next=True,join_by='_',flags=0,keep_original=False):
        '''
        merge wrd with next/previous word. Eg: not with all next words
        :param text:
        :param wrd_list: list of words
        :param join_by:
        :param flags: re flags
        '''
        if with_next:
            for wrd in wrd_list:
                text = re.sub("\\b"+wrd+" ",wrd+join_by,text,flags=flags)
        else:
            for wrd in wrd_list:
                text = re.sub(" "+wrd+"\\b",join_by+wrd,text,flags=flags)
        if keep_original:
            text_bck = text
            for wrd in nltk.word_tokenize(text_bck):
                if join_by in wrd:
                    for wrd1 in wrd.split(join_by):
                        text = text + ' '+wrd1
        return text

    def merge_word_listinput(self,text_list,wrd_list,with_next=True,join_by='_',flags=0,keep_original=False):
        '''
        merge wrd with next/previous word for each sentence in the list. Eg: not with all next words
        :param text_list:
        :param wrd_list:
        :param join_by:
        :param flags: re flags
        '''
        new_list = []
        for text in text_list:
            new_list.append(self.merge_word_textinput(text,wrd_list,with_next,join_by,flags,keep_original))
        return new_list
