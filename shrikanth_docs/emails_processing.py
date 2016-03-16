#!/usr/bin/python
# -*- coding: utf-8 -*-
import pandas as pd,re
from nltk.corpus import stopwords
import pdb

import word_transformations as PPP
import clean_mails

tk = PPP.Tokenizer()

#stop_words=stopwords.words('english')

def load_stop_words(stop_word_file):
    """
    Utility function to load stop words from a file and return as a list of words
    @param stop_word_file Path and file name of a file containing stop words.
    @return list A list of stop words.
    """
    stop_words = []
    for line in open(stop_word_file):
        if line.strip()[0:1] != "#":
            for word in line.split():  # in case more than one per line
                stop_words.append(word)
    return stop_words
stop_words=load_stop_words("SmartStoplist.txt")
stop_words = list(set(stop_words) - set(['not','no']))

def process_mail_text(sent):
    ''' '''
    # sent = clean_mails.fetch_first_mail_text(sent,1)
    # sent = clean_mails.clean_mail_text(sent)
    # unquoted_part_cleaned = sent

    #doc_tokens = tk.split_sent(sent)

    #only lemma
    #doc_tokens_lemma = tk.wordnet_lemma(doc_tokens)
    #only_lemma = ' '.join([' '.join(sent) for sent in doc_tokens_lemma])
    # doc_lemma = tk.wordnet_lemma_textinput(sent)
    # only_lemma = ' '.join(doc_lemma)
    # pdb.set_trace()
    doc_tokens_lemma = tk.split_sent(sent)
    # doc_tokens_lemma = tk.wordnet_lemma(doc_tokens)

    # #lemma porter stem
    # doc_tokens_lemma_porterstem = tk.porter_stemmer(doc_tokens_lemma)
    # lemma_porterstem = ' '.join([' '.join(sent) for sent in doc_tokens_lemma_porterstem])

    #lemma snowball
    doc_tokens_lemma_snowballstem = tk.snowball_stemmer_tokeninput(doc_tokens_lemma)
    lemma_snowballstem = ' '.join([' '.join(sent) for sent in doc_tokens_lemma_snowballstem])

    #lemma stop
    doc_tokens_lemma_stop = tk.stopword_removal_tokeninput(doc_tokens_lemma,stop_words)
    lemma_stop = ' '.join([' '.join(sent) for sent in doc_tokens_lemma_stop])

    #lemma_stop_snowballstem
    doc_tokens_lemma_stop_snowballstem = tk.snowball_stemmer_tokeninput(doc_tokens_lemma_stop)
    lemma_stop_snowballstem = ' '.join([' '.join(sent) for sent in doc_tokens_lemma_stop_snowballstem])


    return (lemma_stop,lemma_snowballstem,lemma_stop_snowballstem)

def process_mail_df(mails,input_column):
    ''' '''
    mails_list = mails[input_column]

    ll0_0,ll0, ll1, ll2, ll3, ll4  = [],[],[],[],[],[]

    for sent in mails_list:
        sent = clean_mails.fetch_first_mail_text(sent,True)
        sent = clean_mails.clean_mail_text(sent)
        # sent = clean_mails.remove_endtext_ner_mail(sent,['suriyah','krishnan','ashwin','ramaswamy'])
        ll0_0.append(sent)

    ll0 = clean_mails.remove_endtext_ner_mail_listinput(ll0_0,['suriyah','krishnan','ashwin','ramaswamy'])
    
    ll1 = tk.wordnet_lemma_listinput(ll0)

    for text in ll1:
        res = process_mail_text(text)
        # ll1.append(res[1])
        ll2.append(res[0])
        ll3.append(res[1])
        ll4.append(res[2])

    mails['unquoted_part_cleaned'] = ll0
    mails['only_lemma']=ll1
    mails['lemma_stop']=ll2
    mails['lemma_snowballstem']=ll3
    mails['lemma_stop_snowballstem']=ll4
    return mails

def process_mails_excel(input_excel="campaign_replies1.xlsx",output_excel="campaign_replies_prepro_tmp1.xls",
                      input_column = 'unquoted_part'):
    ''' '''
    mails = pd.read_excel(input_excel)
    mails = process_mail_df(mails,input_column)
    mails.to_excel(output_excel)

