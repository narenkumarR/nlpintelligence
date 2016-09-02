__author__ = 'joswin'

import pandas as pd
from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:postgres@localhost:5432/linkedin_data')

tmp = pd.read_sql('linkedin_company_base_settu_sir',engine)
text_list = list(tmp['description'])
del tmp

from text_processing import word_transformations
tk = word_transformations.Tokenizer()

text_list_lemma = tk.wordnet_lemma_listinput(text_list) # sometimes

from text_processing import extract_phrases
phr = extract_phrases.PhraseExtractor()

from text_processing import chunking
ch = chunking.Chunker()
chunk_list = ch.ne_chunk_listinput(text_list)

from text_processing import grammar
gm = grammar.Grammar()
grammar_list = gm.parse_regex_grammar_listinput(chunk_list)

from text_processing import extract_phrases
pe = extract_phrases.PhraseExtractor()
phr_list = pe.extract_phrase_treelistinput(grammar_list,['NP','PP','VP','CLAUSE'])

phr_list_all = [j for i in phr_list for j in i]
phr_list_all = list(set(phr_list_all))

pm = extract_phrases.PhraseMerger()

text_phrases = pm.merge_phrases_listinput(text_list,phr_list_all,keep_original=True)

#lda
from sklearn.feature_extraction.text import CountVectorizer,TfidfVectorizer
vectorizer = CountVectorizer(min_df=5,ngram_range=(1,2))
count_vec = vectorizer.fit_transform(text_phrases)
vocab_dict =  {v: k for k, v in vectorizer.vocabulary_.items()}
vocab = [vocab_dict[i] for i in range(max(vocab_dict.keys())+1)]
vocab = ['_'.join(wrds.split()) for wrds in vocab]

import lda
model_10topics = lda.LDA(n_topics=10, n_iter=500, random_state=1)
model_10topics.fit(count_vec)
topic_words = model_10topics.topic_word_
doc_topics = model_10topics.doc_topic_
n_top_words = 20
import numpy as np
for i, topic_dist in enumerate(topic_words):
    topic_words = np.array(vocab)[np.argsort(topic_dist)][:-n_top_words:-1]
    print('Topic {}: {}'.format(i, ' '.join(topic_words)))


###using all words
'''
Topic 0: and of products the in for to is systems product energy solutions quality customers company high are equipment manufacturing as design industry with industrial
Topic 1: the and of in to our we for service with is of_the has as in_the are over all of_the home from have on years
Topic 2: and our to of we in clients the services with business solutions is technology our_clients on development that industry are experience provide team management
Topic 3: and to the of data software for is solutions with management that their mobile in platform by on technology cloud business customers based more
Topic 4: in the and of is more company than has more_than of_the in_the its 000 with of_the companies over as world by global offices headquartered
Topic 5: you your to we and marketing with for or can the it our business are that on is help online have get re will
Topic 6: the to and we of our that in is are people their for by it world as be of_the with this they at in_the
Topic 7: and financial of the to insurance our real estate real_estate is in for with we investment firm credit law services legal are an real_estate
Topic 8: and the of to for health in is care medical healthcare education with programs community services as by through more research that training students
Topic 9: com and www the more visit for on information to at http media is of us more_information for_more digital with content twitter about in
##problem: all stopwords. try removing stopwords
'''
import nltk
from nltk.corpus import stopwords
text_phrases = [i.lower() for i in text_phrases]
vectorizer_stop = CountVectorizer(min_df=5,ngram_range=(1,2),stop_words=stopwords.words())
count_vec = vectorizer_stop.fit_transform(text_phrases)
vocab_dict =  {v: k for k, v in vectorizer_stop.vocabulary_.items()}
vocab = [vocab_dict[i] for i in range(max(vocab_dict.keys())+1)]
vocab = ['_'.join(wrds.split()) for wrds in vocab]

'''
#without phrases
Topic 0: make help one re need time work get offer
Topic 1: solution software data business service technology management customer provide
Topic 2: www http information content twitter platform facebook visit video
Topic 3: service financial insurance real estate provide investment client firm
Topic 4: health care medical healthcare patient service provide hospital life
Topic 5: community program organization education school student public training member
Topic 6: marketing brand business digital design web client service social
Topic 7: product service design industry energy customer quality system company
Topic 8: business client service company work provide help team solution
Topic 9: company service office 000 year one business new include
'''

'''
#added phrases also here
Topic 0: www http information media world visit us content twitter
Topic 1: travel service credit online automotive new car experience sales
Topic 2: marketing clients business design development services web digital sales
Topic 3: community people world education programs mission training of_the research
Topic 4: solutions software data management technology business services customers cloud
Topic 5: health care medical healthcare services patient patients quality hospital
Topic 6: products quality solutions company product systems industry customers high
Topic 7: clients services financial companies business insurance firm industry best
Topic 8: company services new 000 united states estate real of_the
Topic 9: us business service one re help every make time
'''

## look at the cluster and the industires in that cluster
cluster_inds = np.argmax(model_10topics.transform(count_vec),1)
cluster_names = {0:'HR/Job consultancy',1:'Software/cloud/data',2:'Only Social Media Links',3:'Insurance/Financial services/Real estate',4:'Healthcare',5:'Teaching/universities',6:'Marketing/We design',7:'Manufacturing/Industrial related',8:'Technology services',9:'Others'}
clusters = [cluster_names[i] for i in cluster_inds]
tmp['Company Cluster'] = clusters
tmp_group = tmp.groupby(['industry', 'Company Cluster'])
tmp.groupby(['industry', 'Company Cluster']).agg({'industry': len})

