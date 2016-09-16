# recreating works on 15 Sep in home lap
import pandas as pd
df = pd.read_csv('ecommerce_companies.csv')
df = df.dropna()
df.index = range(df.shape[0])
grammar = r"""
  NP1: {<JJ><NN.*>+}          # Chunk sequences of DT, JJ, NN
  NP2: {<NN.*>+<JJ>}          # Chunk sequences of DT, JJ, NN
  NP3: {<NN.*>+}                  #Noun phrases
  VP: {<VB.*><NN.*>+} # Chunk verbs and their arguments
  """
phr_list = ['NP1','NP2','NP3','VP']
tag_list = ['NN','NNS','NNP','NNPS','VB','VBD','VBG','VBN','VBP','VBZ']
import nltk,re
from nltk.corpus import stopwords
from nltk.stem import SnowballStemmer
from text_processing import extract_phrases
stop_words = stopwords.words()+['http','https','goo']
cp = nltk.RegexpParser(grammar)
pe = extract_phrases.PhraseExtractor()
snowball_stemmer = SnowballStemmer('english')
reg_exp = re.compile('[^a-zA-Z ]',re.IGNORECASE)
def tokenizer(text):
    pos_tags = nltk.pos_tag(nltk.word_tokenize(text))
    phrs = pe.extract_phrase_treeinput(cp.parse(pos_tags),['NP1','NP2','VP'])
    wrds = [snowball_stemmer.stem(i[0]) for i in pos_tags if i[1] in tag_list]
    wrds = [wrd for wrd in wrds if wrd not in stop_words]
    phrs = ['_'.join([snowball_stemmer.stem(wrd) for wrd in nltk.word_tokenize(phr)]) for phr in phrs]
    wrds = [reg_exp.sub('',i) for i in wrds]
    return wrds+phrs

from sklearn.feature_extraction.text import TfidfVectorizer
data_samples = df['description']
data_samples = data_samples.dropna()
tfidf_vectorizer = TfidfVectorizer(max_df=0.95, min_df=10,tokenizer=tokenizer)
tfidf = tfidf_vectorizer.fit_transform(data_samples)

from sklearn.decomposition import NMF, LatentDirichletAllocation
n_topics,n_top_words=20,20
def print_top_words(model, feature_names, n_top_words):
    for topic_idx, topic in enumerate(model.components_):
        print("Topic #%d:" % topic_idx)
        print(" ".join([feature_names[i]
                        for i in topic.argsort()[:-n_top_words - 1:-1]]))
    print()

nmf = NMF(n_components=n_topics, random_state=1, alpha=.1, l1_ratio=.5).fit(tfidf)
tfidf_feature_names = tfidf_vectorizer.get_feature_names()
print_top_words(nmf, tfidf_feature_names, n_top_words)

preds = nmf.transform(tfidf)
import numpy as np
preds_ind = np.where(preds>0)


