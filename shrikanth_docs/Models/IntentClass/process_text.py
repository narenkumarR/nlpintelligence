import nltk,re
from word_transformations import Tokenizer

import extract_phrases
from nltk.corpus import stopwords
import clean_mails
from constants import *

tk = Tokenizer()

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

# stop_words=load_stop_words("SmartStoplist.txt")
stop_words=stopwords.words('english')
stop_words = list(set(stop_words+stops_add_list))
stop_words = list(set(stop_words)-set(stops_del_list))

def tokenizer(text):
    wrd_list = nltk.word_tokenize(text.lower())
    # wrd_list = [snowball_stemmer.stem(wrd) for wrd in wrd_list]
    text = ' '.join(wrd_list)
    text = re.sub("[^a-zA-Z_? ]",' ',text)
    wrd_list = nltk.word_tokenize(text)
    wrd_list = [wrd for wrd in wrd_list if len(wrd)>1 or wrd=='?']
    wrd_list = [re.sub(r'^_+|_+$','',wrd) for wrd in wrd_list ]
    return wrd_list

def process_textlist(text_list):
    '''
    :param text_list:
    :return:
    '''
    text_list = [clean_mails.fetch_first_mail_text(sent,False) for sent in text_list]
    text_list = [clean_mails.clean_mail_text(sent) for sent in text_list]
    text_list = clean_mails.remove_endtext_ner_mail_listinput(text_list,remove_endtext_ner_mail_input)
    text_list = [extract_phrases.multiple_replace(replace_phr_input,
        text.lower(),word_limit=True,flags=2) for text in text_list]
    text_list = tk.stopword_removal_listinput(text_list,stop_words)
    text_list = tk.stop_phrase_removal_listinput(text_list,[wrd for wrd in stop_words if len(wrd)>1])
    text_list = tk.stop_phrase_removal_listinput(text_list,stop_phrases)
    phm = extract_phrases.PhraseMerger()
    text_list = phm.merge_phrases_listinput(text_list,merge_phr_list,flags=2)
    text_list = tk.porter_stemmer_listinput(text_list)
    return text_list

