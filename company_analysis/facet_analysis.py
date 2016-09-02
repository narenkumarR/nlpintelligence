import pandas as pd

tmp = pd.read_csv('company_data_for_clustering.csv',sep=';')

from sklearn.decomposition import NMF, LatentDirichletAllocation
n_topics = 50
n_top_words = 20

def print_top_words(model, feature_names, n_top_words):
    for topic_idx, topic in enumerate(model.components_):
        print("Topic #%d:" % topic_idx)
        print(" ".join([feature_names[i]
                        for i in topic.argsort()[:-n_top_words - 1:-1]]))
    print()

from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer

data_samples = tmp['description']
data_samples = data_samples.dropna()
data_samples.index = range(34467)

tfidf_vectorizer = TfidfVectorizer(max_df=0.95, min_df=5, #max_features=n_features,
                                   stop_words='english')
tfidf = tfidf_vectorizer.fit_transform(data_samples)

tf_vectorizer = CountVectorizer(max_df=0.95, min_df=5, 
                                stop_words='english')
tf = tf_vectorizer.fit_transform(data_samples)

nmf = NMF(n_components=n_topics, random_state=1, alpha=.1, l1_ratio=.5).fit(tfidf)
tfidf_feature_names = tfidf_vectorizer.get_feature_names()
print_top_words(nmf, tfidf_feature_names, n_top_words)

lda = LatentDirichletAllocation(n_topics=n_topics, max_iter=5,
                                learning_method='online', learning_offset=50.,
                                random_state=0)
lda.fit(tf)
tf_feature_names = tf_vectorizer.get_feature_names()
print_top_words(lda, tf_feature_names, n_top_words)


#subsumption algorithm
# first level are the specialties
# extra data is the description (remove named entities,keep verbs, nouns and adjectives?)
import re
tmp = tmp.dropna()
specialties = [' '.join([wrd.strip() for wrd in re.split('[ ,&/-]',sp) if wrd.strip()]) for sp in tmp['specialties'] ]
specialties = [i.decode('ascii','ignore') for i in specialties]


descr = list(tmp['description'])
descr = [i.decode('ascii','ignore') for i in descr]
from text_processing.word_transformations import Tokenizer
tk = Tokenizer()
# descr1 = tk.wordnet_lemma_listinput(descr)
import nltk
from text_processing.tagging_methods import get_postag_listinput #(not working properly)
# add specialties words to descr
descr_specialties = [descr[i]+' '+specialties[i] for i in range(len(descr))]
descr_postags = get_postag_listinput(descr_specialties)
# descr_postags = [nltk.pos_tag(nltk.word_tokenize(txt)) for txt in descr]
descr_chunks = [nltk.ne_chunk(i) for i in descr_postags]

# remove trees, keep verbs, nouns and adjectives
descr_chunks1 = [[i for i in chunk if type(i) == tuple] for chunk in descr_chunks ]
from nltk.stem import WordNetLemmatizer
wnl = WordNetLemmatizer()
from text_processing.word_transformations import get_wordnet_pos
descr_chunks1 = [[wnl.lemmatize(wrd,get_wordnet_pos(tag)) for wrd,tag in i if re.search('^V|^N|^J',tag)] for i in descr_chunks1]
# descr_chunks1 = [list(set(i)) for i in descr_chunks1]
descr_sents = [' '.join(i) for i in descr_chunks1]
specialties_sents = [' '.join([wnl.lemmatize(i) for i in nltk.word_tokenize(txt)]) for txt in specialties]

descr_sents_1 = [re.sub(' +',' ',re.sub('[^a-zA-Z ]',' ',sent)) for sent in descr_sents]

# save
# import pickle
# with open('processed_texts.pkl','w') as f:
#     pickle.dump({'descr_postags':descr_postags,'descr_chunks':descr_chunks,'descr_sents':descr_sents},f)

# add wordnet synsets? in first version, do without this

# convert the texts to matrix using vectorizer.
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
df_vectorizer = CountVectorizer(max_df=0.95, min_df=5, #max_features=n_features,
                                   stop_words='english')
binary_vectorizer = CountVectorizer(max_df=0.95, min_df=5, #max_features=n_features,
                                   stop_words='english',binary=True)
# orig_vectorizer = CountVectorizer(max_df=0.95, min_df=5, #max_features=n_features,
#                                    stop_words='english')
mat_context = df_vectorizer.fit_transform(descr_sents_1)
mat_orig = df_vectorizer.transform(specialties_sents)
# without any processing
# mat_context = df_vectorizer.fit_transform(descr_specialties)
# mat_orig = df_vectorizer.transform(specialties)

# Frequency-based Shifting:
import numpy as np,scipy
def get_freq_shift(mat_context,mat_orig):
    '''freq_context - freq_orig (freq : count wrd/total wrds)
    :param mat_context:
    :param wrds_context:
    :param mat_orig:
    #:param wrds_orig:should be same as wrds_context
    :return: matrix : wrds,freq_shift_value
    '''
    # all_wrds = list(set(wrds_context+wrds_orig))
    context_tot_cnt = float(scipy.sparse.csr_matrix.sum(mat_context))
    orig_tot_cnt = float(scipy.sparse.csr_matrix.sum(mat_orig))
    context_cnts = scipy.sparse.csr_matrix.sum(mat_context,0)
    orig_cnts = scipy.sparse.csr_matrix.sum(mat_orig,0)
    freq_shift_val = context_cnts/context_tot_cnt - orig_cnts/orig_tot_cnt
    # freq_shift_val = context_cnts - orig_cnts
    return freq_shift_val.transpose()

from scipy.stats import rankdata
def get_rank_shift(mat_context,mat_orig):
    ''' log2(rank_orig) - log2(rank_context) (rank :order each word by frequency )
    :param mat_context:
    :param wrds_context:
    :param mat_orig:
    :param wrds_orig:
    :return:matrix : wrds,rank_shift_value
    '''
    context_cnts = scipy.sparse.csr_matrix.sum(mat_context,0)
    orig_cnts = scipy.sparse.csr_matrix.sum(mat_orig,0)
    context_ranks = (1+context_cnts.shape[1]) - rankdata(context_cnts,method='min')
    orig_ranks = (1+orig_cnts.shape[1]) - rankdata(orig_cnts,method='min')
    context_ranks_log2 = np.log(context_ranks)/np.log(2)
    orig_ranks_log2 = np.log(orig_ranks)/np.log(2)
    return orig_ranks_log2-context_ranks_log2

df = pd.DataFrame({'word':df_vectorizer.get_feature_names()})
df['freq_shift'] = get_freq_shift(mat_context,mat_orig)
df['rank_shift'] = get_rank_shift(mat_context,mat_orig)

# next step is to find significant terms using log likelihood test
# If the null hypothesis holds, then the log-likelihood ratio is asymptotically X2 dis-
# tributed with k/2 - 1 degrees of freedom. When j is 2 (the binomial), - 2 log L~ will be
# X2 distributed with one degree of freedom.

def log_likelihood(p,k,n):
    '''
    :param p:
    :param k:
    :param n:
    :return:
    '''
    return k*np.log(p+0.000000000000001) + (n-k)*np.log(1-(p+0.000000000000001))

def get_log_likelihood_statistic(mat_context,mat_orig):
    '''
     for word t, dfC is word count in context data, df is word count in original data
     and D is the total number of words in the original data
     − log λt = log L(p1 , dfC , |D|) + log L(p2 , df , |D|)− log L(p, df , |D|) − log L(p, dfC , |D|)
     log L(p, k, n) = k log(p) + (n − k) log(1 − p),
     p1 = dfC/|D| , p2 = df/|D| , and p = (p1 +p2 .)/2
    :param mat_context:
    :param mat_orig:
    :return:
    '''
    d_count = float(scipy.sparse.csr_matrix.sum(mat_orig))
    context_cnts = scipy.sparse.csr_matrix.sum(mat_context,0)
    orig_cnts = scipy.sparse.csr_matrix.sum(mat_orig,0)
    p1 = context_cnts/d_count
    p2 = orig_cnts/d_count
    p = (p1+p2)/2
    log_likelihood_fun = log_likelihood(np.asarray(p1),np.asarray(context_cnts),d_count) + \
                         log_likelihood(np.asarray(p2),np.asarray(orig_cnts),d_count) - \
                         log_likelihood(np.asarray(p),np.asarray(context_cnts),d_count) - \
                         log_likelihood(np.asarray(p),np.asarray(orig_cnts),d_count)
    return 2*log_likelihood_fun.transpose()

df['log_likelihood'] = get_log_likelihood_statistic(mat_context,mat_orig)

# p value calculation
df['p_value'] = 1 - scipy.stats.chi2.cdf(df['log_likelihood'], 1)

# words of facets : when freq_shif>0,rank_shift>0 and p_value<0.05
df_wrds = df[(df['freq_shift']>0)&(df['rank_shift']>0)&(df['p_value']<0.05)]

import facet_gen
fwg = facet_gen.FacetWordsGen()
