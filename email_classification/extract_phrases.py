#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'joswin'

import nltk
import pdb

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

    def extract_phrase_treeinput(self,tr,label,stopwords=[]):
        '''
        :param tr: tree object
        :param label: label to extract phrase
        :param stopwords: stopwords list
        :return:
        '''
        temp_list=[]
        for subtree in tr.subtrees():
            if subtree.label()== label:
                tree_data = tree_travel(subtree,stopwords)
                temp_list+=[' '.join((e for e in list(tree_data)))]
        return temp_list

    def extract_phrase_treelistinput(self,ltrees,label,stopwords=[]):
        '''
        :param ltrees: list of tree objects
        :param label: label to extract phrase
        :param stopwords:
        :return:
        '''
        outlist = []
        for tr in ltrees:
            outlist.append(self.extract_phrase_treeinput(tr,label,stopwords))
        return outlist

