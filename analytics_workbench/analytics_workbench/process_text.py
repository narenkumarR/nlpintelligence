__author__ = 'joswin'
# -*- coding: utf-8 -*-

import re
from pandas import Series
from nltk import word_tokenize,pos_tag,RegexpParser
from nltk.corpus import stopwords
from nltk.stem import SnowballStemmer,WordNetLemmatizer
from sklearn.feature_extraction.text import CountVectorizer,TfidfVectorizer

from .extract_phrases import PhraseExtractor
# from timex import check_if_date_present

def multiple_replace(dict, text, word_limit = False, flags = 0):
    '''replaces multiple matches
    :param dict: diction with key as the phrase to be replaced and value as the phrase by which it needs to replaced
    :param text: text input
    :param word_limit: should the phrases be contained between world limits (eg: if trying to match 'def', if True,
                'abcdefghi' will not be matched. If False, 'abcdefghi' will be matched)
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

stop_words_default = stopwords.words()+['http','https','goo','isnt','wwwfacebookcomtr','wwwgoogletagmanagercomnshtml',
                               'and','be','do','facebook','for','in','is','inc','linkedin','of','the','to']
grammar = r"""
  NP1: {<JJ><NN.*>+}          # Chunk sequences of JJ, NN
  NP2: {<NN.*>+<JJ>}          # Chunk sequences of NN and JJ
  NP3: {<NN.*>+}                  #Noun phrases
  VP: {<VB.*><NN.*>+} # Chunk verbs and their arguments
  """
phr_list = ['NP1','NP2','NP3','VP']
tag_list = ['NN','NNS','NNP','NNPS','VB','VBD','VBG','VBN','VBP','VBZ']
cp = RegexpParser(grammar)
pe = PhraseExtractor()
snowball_stemmer = SnowballStemmer('english')
wordnet_lemmatizer = WordNetLemmatizer()
reg_exp = re.compile('[^a-zA-Z ]',re.IGNORECASE)

def tokenizer(text,stem_type='lemmatize',phrase_generation=False,stop_words = []):
    '''This function takes a text as input and return a list of words after stemming/lemmatization,
              stop word removal and phrases if phrase_generation is True
    stem_type : lemmatize/stem
    phrase_generation : should phrases generated using grammar
    stop_words : list of words to be removed
    '''
    # assert stem_type in ['lemmatize','stem']
    if not stop_words:
        stop_words = stop_words_default
    if stem_type or phrase_generation:#if stemming and phrase generation is not needed, no need to do pos-tagging
        pos_tags = pos_tag(word_tokenize(text))
        if stem_type == 'stem':
            wrds = [snowball_stemmer.stem(i[0]) for i in pos_tags if i[1] in tag_list]
        elif stem_type == 'lemmatize':
            wrds = [wordnet_lemmatizer.lemmatize(i[0]) for i in pos_tags if i[1] in tag_list]
        else:#directly use the word
            wrds = [i[0] for i in pos_tags if i[1] in tag_list]
        # stopword removal
        wrds = [wrd for wrd in wrds if wrd.lower() not in stop_words]
        if phrase_generation:
            phrs = pe.extract_phrase_treeinput(cp.parse(pos_tags),['NP1','NP2','VP'])
            if stem_type == 'stem':
                phrs = ['_'.join([snowball_stemmer.stem(wrd) for wrd in word_tokenize(phr)]) for phr in phrs]
            elif stem_type == 'lemmatize':
                phrs = ['_'.join([wordnet_lemmatizer.lemmatize(wrd) for wrd in word_tokenize(phr)]) for phr in phrs]
            else:
                phrs = ['_'.join([wrd for wrd in word_tokenize(phr)]) for phr in phrs]
            wrds = [reg_exp.sub('',i) for i in wrds]
            return wrds+phrs
        else:
            return wrds
    else:
        return [wrd for wrd in word_tokenize(text) if wrd.lower() not in stop_words]

class ProcessText(object):
    '''
    '''
    def __init__(self):
        pass

    def load_synonyms(self,loc):
        '''
        :param loc: each line should contain synonyms comma-separated. the first word will be choosen as the default word
        :return:
        '''
        synonym_dic = {}
        with open(loc,'r') as f:
            for line in f:
                if line[-1] == '\n':
                    line = line[:-1]
                words = line.split(',')
                if len(words) < 2:
                    continue
                default_wrd = words[0]
                for word in words[1:]:
                    synonym_dic[word] = default_wrd
        return synonym_dic

    def load_words_from_file(self,loc):
        '''
        :param loc:
        :return:
        '''
        with open(loc,'r') as f:
            words = f.readlines()
        words = [word[:-1] if word[-1]=='\n' else word for word in words ]
        return words

    def gen_document_term_matrix(self,text_documents,vectorizer_type='Count',synonyms_dic={},stem_type='lemmatize',
                                 phrase_generation=False,
                stop_words=[],lower=True,n_gram_range=(1,2),max_df=0.9,min_df=0.01,vocabulary=None,**kwargs):
        '''
        :param text: input text
        :param vectorizer_type: 'Count' or 'Tfidf'
        :param lower: change to lower case
        :param stop_words: list of words to remove
        :param synonyms: synonyms
        :param stem_type:
        :param n_gram_range:tuple (ngram_min,ngram_max)
        :param max_df:  discard words present more than this value in the data (eithe ratio or number of times)
        :param min_df: discard words below this threshold
        :param vocabulary: default vocabulary to use
        :return:
        '''
        self.synonyms_dic = synonyms_dic
        # self.stem_type = stem_type
        # self.phrase_generation = phrase_generation
        # self.stop_words = stop_words
        # self.lower = lower
        # self.n_gram_range = n_gram_range
        # self.max_df = max_df
        # self.min_df = min_df
        # self.vocabulary = vocabulary
        # self.kwargs = kwargs
        text_series = Series(text_documents)
        text_series = text_series.fillna('').str.lower()
        if synonyms_dic:
            # text = multiple_replace(synonyms,text_documents,word_limit=True,flags=re.IGNORECASE)
            text_series = text_series.apply(lambda text : 
                                            multiple_replace(synonyms_dic,text,word_limit=True,flags=re.IGNORECASE))
        if vectorizer_type == 'Count':
            self.vectorizer = CountVectorizer(decode_error='ignore',
                    tokenizer=lambda text: tokenizer(text,stem_type=stem_type,phrase_generation=phrase_generation,
                                                                 stop_words=stop_words),
                    lowercase=lower,ngram_range=n_gram_range,max_df=max_df,min_df=min_df,vocabulary=vocabulary,**kwargs)
        else:
            self.vectorizer = TfidfVectorizer(decode_error='ignore',
                    tokenizer=lambda text: tokenizer(text,stem_type=stem_type,phrase_generation=phrase_generation,
                                                                 stop_words=stop_words),
                    lowercase=lower,ngram_range=n_gram_range,max_df=max_df,min_df=min_df,vocabulary=vocabulary,**kwargs)
        dtm = self.vectorizer.fit_transform(text_series)
        self.vocabulary = self.get_vectorizer_vocabulary(self.vectorizer)
        return dtm,self.vocabulary
    
    def get_vectorizer_vocabulary(self,vectorizer):
        vocab_list = [0]*len(vectorizer.vocabulary_)
        for wrd in vectorizer.vocabulary_:
            vocab_list[vectorizer.vocabulary_[wrd]] = wrd
        return vocab_list

    def gen_dtm_from_files(self,text_documents,vectorizer_type='Count',synonym_loc=None,stem_type='lemmatize',
                                 phrase_generation=False,
                stop_words_loc=None,lower=True,n_gram_range=(1,2),max_df=0.9,min_df=0.01,vocabulary_loc=None,**kwargs):
        '''use this function to get document term matrix , when the stopwords, synonyms etc are available in files
        :param text_documents:
        :param vectorizer_type:Count or Tfidf
        :param synonym_loc: location of synonym file
        :param stem_type: lemmatize or stem or None(no processing)
        :param phrase_generation: generate phrases if True
        :param stop_words_loc: location of stop words
        :param lower: change to lowercase if True
        :param n_gram_range: tuple (ngram_min,ngram_max)
        :param max_df: words above this threshold are removed. either fraction or number
        :param min_df: words below this threashold are removed
        :param vocabulary_loc: if location provided, words in the file will be used
        :return: document term matrix
        '''
        if synonym_loc:
            synonyms_dic = self.load_synonyms(synonym_loc)
        else:
            synonyms = None
        if stop_words_loc:
            stop_words = self.load_words_from_file(stop_words_loc)
        else:
            stop_words = None
        if vocabulary_loc:
            self.vocabulary = self.load_words_from_file(vocabulary_loc)
        else:
            self.vocabulary = None
        return self.gen_document_term_matrix(
            text_documents=text_documents,vectorizer_type=vectorizer_type,synonyms_dic=synonyms_dic,stem_type=stem_type,
            phrase_generation=phrase_generation,stop_words=stop_words,lower=lower,n_gram_range=n_gram_range,
            max_df=max_df,min_df=min_df,vocabulary=self.vocabulary,**kwargs
        )
    
    def transform_text_list(text_list):
        '''use this function to generate a document term matrix for a text list using existing vectorizer. ie, the 
         vectorizer should be generated already using a text input'''
        text_series = Series(text_documents)
        if self.synonyms_dic:
            # text = multiple_replace(synonyms,text_documents,word_limit=True,flags=re.IGNORECASE)
            text_series = text_series.apply(lambda text : 
                                            multiple_replace(self.synonyms_dic,text,word_limit=True,flags=re.IGNORECASE))
        dtm = self.vectorizer.transform(text_series)
        return dtm,self.vocabulary
        