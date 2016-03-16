__author__ = 'joswin'
#!/usr/bin/python
# -*- coding: utf-8 -*-

import nltk,re,pdb
from nltk.stem import WordNetLemmatizer,PorterStemmer,SnowballStemmer
from nltk.corpus import wordnet

import tagging_methods

wnl = WordNetLemmatizer()
porter_stemmer = PorterStemmer()
snowball_stemmer = SnowballStemmer('english')

def get_wordnet_pos(treebank_tag):
    if treebank_tag.startswith('J'):
        return wordnet.ADJ
    elif treebank_tag.startswith('V'):
        return wordnet.VERB
    elif treebank_tag.startswith('N'):
        return wordnet.NOUN
    elif treebank_tag.startswith('R'):
        return wordnet.ADV
    else:
        return wordnet.NOUN

class Tokenizer(object):
    def __init__(self):
        '''
        :return:
        '''
        pass

    def split_sent(self,doc):
        '''
        :param doc:
        :return:
        '''
        doc_tokens = [[wrd for wrd in nltk.word_tokenize(sent)] for sent in nltk.sent_tokenize(doc)]
        return doc_tokens

    def merge_token(self,tokens):
        ''' merge tokens to recreate sentence
        :param tokens:
        :return:
        '''
        return ' '.join([' '.join(sent) for sent in tokens])

    def wordnet_lemma_tokeninput(self,doc_tokens):
        '''
        :param doc_tokens: Token input [[wrd11,wrd12,wrd13,..],[wrd21,wrd22,wrd23,...],...]
        :return:
        '''
        #lemmatizing
        sents = [' '.join(sent) for sent in doc_tokens]
        sents_lemma = self.wordnet_lemma_listinput(sents)
        doc_lemma = [[wrd for wrd in nltk.word_tokenize(sent)]for sent in sents_lemma]
        return doc_lemma

    def wordnet_lemma_textinput(self,text):
        ''' Text input
        :param text:
        :return:
        '''
        doc_tags = nltk.pos_tag(nltk.word_tokenize(text))
        doc_tokens_lemma = [wnl.lemmatize(wrd,get_wordnet_pos(tag)) for wrd,tag in doc_tags]
        return ' '.join(doc_tokens_lemma)

    def wordnet_lemma_listinput(self,text_list):
        ''' for list like inputs
        :param text_list: list of sentences
        :return:
        '''
        doc_tags = tagging_methods.get_postag_listinput(text_list)
        tokens_lemma=[[wnl.lemmatize(wrd,get_wordnet_pos(tag)) for wrd,tag in sent] for sent in doc_tags]
        tokens_lemma_list = [' '.join(sent) for sent in tokens_lemma]
        return tokens_lemma_list

    def stopword_removal_tokeninput(self,doc_tokens,mystops=[]):
        '''
        :param doc_tokens:
        :return:
        '''
        doc_stop = [[wrd for wrd in sent if wrd.lower() not in mystops] for sent in doc_tokens]
        return doc_stop

    def stopword_removal_textinput(self,text,mystops=[]):
        '''
        :param text:
        :param mystops:
        :return:
        '''
        text_tokens = self.split_sent(text)
        doc_stop = self.stopword_removal_tokeninput(text_tokens,mystops)
        return self.merge_token(doc_stop)

    def stopword_removal_listinput(self,doc_list,mystops=[]):
        '''
        :param doc_list:
        :return:
        '''
        doc_stop = [self.stopword_removal_textinput(sent,mystops) for sent in doc_list]
        return doc_stop

    def stop_phrase_removal_textinput(self,doc,myphrases):
        '''
        :param doc:
        :param myphrases:
        :return:
        '''
        regg = re.compile('|'.join(myphrases))
        doc = regg.sub('',doc)
        return doc

    def porter_stemmer_tokeninput(self,doc_tokens):
        '''
        :param doc_tokens:
        :return:
        '''
        doc_stem = [[porter_stemmer.stem(wrd) for wrd in sent] for sent in doc_tokens]
        return doc_stem

    def porter_stemmer_textinput(self,text):
        '''
        :param doc_tokens:
        :return:
        '''
        doc_tokens = self.split_sent(text)
        doc_stem = self.porter_stemmer_tokeninput(doc_tokens)
        return self.merge_token(doc_stem)

    def snowball_stemmer_tokeninput(self,doc_tokens):
        '''
        :param doc_tokens:
        :return:
        '''
        doc_stem = [[snowball_stemmer.stem(wrd) for wrd in sent] for sent in doc_tokens]
        return doc_stem

    def snowball_stemmer_listinput(self,doc_list):
        '''
        :param doc_list:
        :return:
        '''
        doc_stem = [self.snowball_stemmer_textinput(sent) for sent in doc_list]
        return doc_stem

    def snowball_stemmer_textinput(self,text):
        '''
        :param doc_tokens:
        :return:
        '''
        doc_tokens = self.split_sent(text)
        doc_stem = self.snowball_stemmer_tokeninput(doc_tokens)
        return self.merge_token(doc_stem)

    def stemmer_generic_tokeninput(self,doc_tokens,stemmer):
        '''
        Takes a stemmer function as input and output the stemmed respons
        :param doc_tokens:
        :param stemmer:
        :return:
        '''
        doc_stem = [[stemmer(wrd) for wrd in sent] for sent in doc_tokens]
        return doc_stem

    def stemmer_generic_listinput(self,text,stemmer):
        '''
        :param text:
        :param stemmer:
        :return:
        '''
        doc_tokens = self.split_sent(text)
        doc_stem = self.stemmer_generic_tokeninput(doc_tokens,stemmer)
        return self.merge_token(doc_stem)

