#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'joswin'
import nltk,re

from text_processing.word_transformations import Tokenizer
from text_processing import extract_phrases
# from naive_bayes.constants import *

tk = Tokenizer()

def tokenizer(text):
    wrd_list = nltk.word_tokenize(text.lower())
    # wrd_list = [snowball_stemmer.stem(wrd) for wrd in wrd_list]
    text = ' '.join(wrd_list)
    text = re.sub("[^a-zA-Z_? ]",' ',text)
    wrd_list = nltk.word_tokenize(text)
    wrd_list = [wrd for wrd in wrd_list if len(wrd)>1 or wrd=='?']
    wrd_list = [re.sub(r'^_+|_+$','',wrd) for wrd in wrd_list ]
    return wrd_list

def process_textlist(text_list,stem,replace_phr_input,stop_words,stop_phrases,merge_phr_list_keep_original,
                     merge_phr_list_remove_original,merge_words_with_next_keep_original,merge_words_with_next_remove_original):
    '''
    :param text_list:
    :param stem: Stem words if true
    :param replace_phr_input: (dict) key:value - phrases: replacement
    :param stop_words: list of stopwords
    :param stop_phrases: list of phrases to be removed
    :param merge_phr_list_keep_original: list of phrases to be merged(keep original words also)
    :param merge_phr_list_remove_original: list of phrases to be merged (originial words in the phrases will be removed)
    :param merge_words_with_next_keep_original:(list of words) phrases will be created using these words with the next word
                eg: ['no','stop'] -> "no interest" will be transformed to "no_interest no interest"
    :param merge_words_with_next_remove_original: (list of words) same as merge_words_with_next_keep_original, the original words will be removed
    :return:
    '''
    # extracting phrases
    text_list = [extract_phrases.multiple_replace(replace_phr_input,
        text.lower(),word_limit=True,flags=2) for text in text_list]
    text_list = tk.stopword_removal_listinput(text_list,stop_words)
    text_list = tk.stop_phrase_removal_listinput(text_list,[wrd for wrd in stop_words if len(wrd)>1])
    text_list = tk.stop_phrase_removal_listinput(text_list,stop_phrases)
    phm = extract_phrases.PhraseMerger()
    # merge phrases in the list. This is manually created phrase list, so may not generalize well
    # note: keep original =True cases should be run first
    text_list = phm.merge_phrases_listinput(text_list,merge_phr_list_keep_original,flags=2,keep_original=True)
    text_list = phm.merge_phrases_listinput(text_list,merge_phr_list_remove_original,flags=2,keep_original=False)
    # note: keep original =True cases should be run first
    text_list = phm.merge_word_listinput(text_list,merge_words_with_next_keep_original,flags=re.IGNORECASE,keep_original=True)
    text_list = phm.merge_word_listinput(text_list,merge_words_with_next_remove_original,flags=re.IGNORECASE,keep_original=False)
    #check if date time is present in the text.if yes, put a field as 1,else 0
    # text_list = [txt+' datetime_in_text_xxx' if timex.check_if_date_present(txt) else txt for txt in text_list]
    if stem:
        text_list = tk.porter_stemmer_listinput(text_list)
    return text_list

