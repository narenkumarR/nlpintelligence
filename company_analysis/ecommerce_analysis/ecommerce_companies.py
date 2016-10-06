__author__ = 'joswin'

import pandas as pd,numpy as np
from sqlalchemy import create_engine
# create table tmp_table_ecommerce_companies as select  * from linkedin_company_base where specialties ~* 'e(.)?commerce' ;
engine = create_engine('postgresql://pipecandy_user:pipecandy@192.168.1.142:5432/pipecandy_db1')
df = pd.read_sql_query("select distinct on (website) linkedin_url,description,specialties,website from (select  * from tmp_table_ecommerce_companies)a",con=engine)

# clustering
from sklearn.decomposition import NMF, LatentDirichletAllocation
n_topics = 20
n_top_words = 20
def print_top_words(model, feature_names, n_top_words,neg_wrds=False):
    for topic_idx, topic in enumerate(model.components_):
        print("Topic #%d:" % topic_idx)
        if neg_wrds:
            print("Most contribution")
        print(" ".join([feature_names[i]
                        for i in topic.argsort()[:-n_top_words - 1:-1]]))
        if neg_wrds:
            print("Least contribution")
            print(" ".join([feature_names[i]
                            for i in topic.argsort()[:n_top_words]]))
    print()

from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer

data_samples = df['description']
data_samples = data_samples.dropna()
tfidf_vectorizer = TfidfVectorizer(max_df=0.95, min_df=20, #max_features=n_features,
                                   stop_words='english')
tfidf = tfidf_vectorizer.fit_transform(data_samples)

nmf = NMF(n_components=n_topics, random_state=1, alpha=.1, l1_ratio=.5).fit(tfidf)
tfidf_feature_names = tfidf_vectorizer.get_feature_names()
print_top_words(nmf, tfidf_feature_names, n_top_words)

pred = nmf.transform(tfidf)
pd.Series(np.argmax(pred,1)).value_counts()

# one cluster (13) seems related to e-commerce. cluster this again
n_topics = 10
df1 = df.ix[np.where(np.argmax(pred,1)==3)[0],df.columns[:-3]]
df1.index = range(df1.shape[0])
tfidf1 = tfidf_vectorizer.fit_transform(df1['description'])
nmf1 = NMF(n_components=n_topics, random_state=1, alpha=.1, l1_ratio=.5).fit(tfidf1)
tfidf_feature_names = tfidf_vectorizer.get_feature_names()
print_top_words(nmf1, tfidf_feature_names, n_top_words)
pred = nmf.transform(tfidf1)
df1.ix[np.where(np.in1d(np.argmax(pred,1),[3,4,7,8]))[0],df1.columns[:-3]].to_excel('ecommerce_companies_final.xls',index=False)

# try latent semantic analysis
from sklearn.decomposition import TruncatedSVD
svd = TruncatedSVD(n_components=100, random_state=42)
lsa_out = svd.fit_transform(tfidf)
print_top_words(svd, tfidf_feature_names, n_top_words)

# try gensim
from gensim.models.doc2vec import TaggedDocument
from gensim.models import Doc2Vec
from gensim import utils
import nltk

# training
# class LabeledSentenceMaker(object):
#     def __init__(self, sources,labels):
#         self.sources = sources
#         self.labels = labels
#         flipped = {}
#         # make sure that keys are unique
#         for key, value in sources.items():
#             if value not in flipped:
#                 flipped[value] = [key]
#             else:
#                 raise Exception('Non-unique prefix encountered')
#
#     def __iter__(self):
#         for source, prefix in self.sources.items():
#             with utils.smart_open(source) as fin:
#                 for item_no, line in enumerate(fin):
#                     yield LabeledSentence(utils.to_unicode(line).split(), [prefix + '_%s' % item_no])
#
#     def to_array(self):
#         self.sentences = []
#         for source, prefix in self.sources.items():
#             with utils.smart_open(source) as fin:
#                 for item_no, line in enumerate(fin):
#                     self.sentences.append(LabeledSentence(utils.to_unicode(line).split(), [prefix + '_%s' % item_no]))
#         return self.sentences
#
#     def sentences_perm(self):
#         shuffle(self.sentences)
#         return self.sentences

# sent_list = [LabeledSentence(nltk.word_tokenize(utils.to_unicode(df.ix[ind,1])),utils.to_unicode(df.ix[ind,0])) for ind in range(df.shape[0])]
sent_list = [TaggedDocument(words=nltk.word_tokenize(utils.to_unicode(df.ix[ind,1])),tags=[utils.to_unicode(df.ix[ind,0])]) for ind in range(df.shape[0])]
model = Doc2Vec(min_count=1, window=10, size=100, sample=1e-5, negative=5, workers=8)
model.build_vocab(sent_list)

from random import shuffle
for epoch in range(10):
    shuffle(sent_list)
    model.train(sent_list)

st = nltk.word_tokenize('india companies ecommerce')
new_doc_vec = model.infer_vector(st)
best = model.docvecs.most_similar([new_doc_vec])
print best
# not giving good results.. need to use more details


# using probability and find topics
pred_probs = (pred.transpose()/np.sum(pred,1)).transpose()
pos_cases = np.where(pred_probs>0.05)
pos_cases_df = pd.DataFrame({'row':pos_cases[0],'col':pos_cases[1]})
pos_cases_ind = pos_cases_df.groupby('row')['col'].apply(list)

# do this after using grammar
grammar = r"""
  NP1: {<JJ><NN.*>+}          # Chunk sequences of DT, JJ, NN(removed DT)
  NP2: {<NN.*><JJ>+}          # Chunk sequences of DT, JJ, NN(removed DT)
  VP: {<VB.*><NN.*>} # Chunk verbs and their arguments
  CLAUSE: {<NP><VP>}           # Chunk NP, VP
  """
from text_processing import extract_phrases
cp = nltk.RegexpParser(grammar,loop=1)
pe = extract_phrases.PhraseExtractor()
pe.extract_phrase_treeinput(cp.parse(nltk.pos_tag(nltk.word_tokenize(df['description'][0]))),
                            ['NP1','NP2','VP','CLAUSE','NNP','NN','NNS','NNPS'])#,'VB','VBD','VBG','VBN','VBP','VBZ'
import nltk,re
from nltk.stem import SnowballStemmer
from nltk.corpus import stopwords
snowball_stemmer = SnowballStemmer('english')
phr_list = ['NP1','NP2','VP','CLAUSE','NNP','NN','NNS','NNPS']
tag_list = ['NNP','NN','NNS','NNPS']
stop_wrds = stopwords.words() + ['http','https','www','goo']
reg_exp = re.compile('[^a-zA-Z ]',re.IGNORECASE)
def tokenizer(text):
    pos_tags = nltk.pos_tag(nltk.word_tokenize(text))
    phrs = pe.extract_phrase_treeinput(cp.parse(pos_tags),phr_list)
    wrds = [i[0] for i in pos_tags if i[1] in tag_list ]
    wrds_stemmed = [reg_exp.sub('',snowball_stemmer.stem(wrd)) for wrd in wrds]
    wrds_stemmed = [wrd for wrd in wrds_stemmed if wrd not in stop_wrds]
    phrs_stemmed = ['_'.join([reg_exp.sub('',snowball_stemmer.stem(wrd)) for wrd in nltk.word_tokenize(phr)]) for phr in phrs]
    return wrds_stemmed+phrs_stemmed

tfidf_vectorizer = TfidfVectorizer(max_df=0.95, min_df=10, #max_features=n_features,
                                   stop_words='english',tokenizer=tokenizer)
tfidf = tfidf_vectorizer.fit_transform(data_samples)

nmf = NMF(n_components=n_topics, random_state=1, alpha=.1, l1_ratio=.5).fit(tfidf)
tfidf_feature_names = tfidf_vectorizer.get_feature_names()
print_top_words(nmf, tfidf_feature_names, n_top_words)
