#add utf encoding here
__author__ = 'joswin'

import nltk,re
from nltk.corpus import stopwords
from nltk.stem import SnowballStemmer
from nltk.stem.wordnet import WordNetLemmatizer
from text_processing import extract_phrases
import pandas as pd
import numpy as np
import pickle
from sklearn.feature_extraction.text import TfidfVectorizer,CountVectorizer
from sklearn.decomposition import NMF, LatentDirichletAllocation
from itertools import izip

grammar = r"""
  NP1: {<JJ><NN.*>+}          # Chunk sequences of JJ, NN
  NP2: {<NN.*>+<JJ>}          # Chunk sequences of NN and JJ
  NP3: {<NN.*>+}                  #Noun phrases
  VP: {<VB.*><NN.*>+} # Chunk verbs and their arguments
  """
# phr_list = ['NP1','NP2','NP3','VP']
phr_list = ['NP1','NP2','VP']
tag_list = ['NN','NNS','NNP','NNPS','VB','VBD','VBG','VBN','VBP','VBZ']
stop_words = stopwords.words()+['http','https','goo','isnt']
cp = nltk.RegexpParser(grammar)
pe = extract_phrases.PhraseExtractor()
snowball_stemmer = SnowballStemmer('english')
wordnet_lemmatizer = WordNetLemmatizer()
reg_exp = re.compile('[^a-zA-Z ]',re.IGNORECASE)

nmf_topics_sheetname,lda_topics_sheetname = 'nmf_topics_sheet','lda_topics_sheet'

def save_to_excel(df_list,sheet_name_list,out_loc):
    '''
    :param df_list:
    :param sheet_name_list:
    :param out_loc:
    :return:
    '''
    if not sheet_name_list:
        sheet_name_list = ['sheetno_{}'.format(i) for i in range(len(df_list))]
    writer = pd.ExcelWriter(out_loc)
    for df,sheet_name in izip(df_list,sheet_name_list):
        df.to_excel(writer,sheet_name=sheet_name,index=False)
    writer.save()

def tokenizer(text,stem_type='lemmatize'):
    '''
    :param text:
    :param stem_type: type of stemming to be done
    :return:
    '''
    pos_tags = nltk.pos_tag(nltk.word_tokenize(text))
    phrs = pe.extract_phrase_treeinput(cp.parse(pos_tags),phr_list)
    if stem_type == 'stem':
        wrds = [snowball_stemmer.stem(i[0]) for i in pos_tags if i[1] in tag_list]
    elif stem_type == 'lemmatize':
        wrds = [wordnet_lemmatizer.lemmatize(i[0]) for i in pos_tags if i[1] in tag_list]
    else:
        wrds = [i[0] for i in pos_tags if i[1]in tag_list]
    wrds = [wrd for wrd in wrds if wrd not in stop_words]
    if stem_type == 'stem':
        phrs = ['_'.join([snowball_stemmer.stem(wrd) for wrd in nltk.word_tokenize(phr)]) for phr in phrs]
    elif stem_type == 'lemmatize':
        phrs = ['_'.join([wordnet_lemmatizer.lemmatize(wrd) for wrd in nltk.word_tokenize(phr)]) for phr in phrs]
    else:
        phrs = ['_'.join([wrd for wrd in nltk.word_tokenize(phr)]) for phr in phrs]
    wrds = [reg_exp.sub('',i) for i in wrds]
    return wrds+phrs

def get_processed_df(inp_file_loc,text_cols = ('description1','description2'),stem_type='lemmatize',index_col='ind'):
    '''
    :param inp_file_loc:
    :param text_cols:
    :param stem_type:
    :return:
    '''
    df = pd.read_csv(inp_file_loc,sep=None)
    df = df.dropna()
    data_samples = df[text_cols[0]]
    if len(text_cols)>1:
        for col in text_cols[1:]:
            data_samples = data_samples + df[col]
    data_samples = data_samples.apply(lambda x: ' '.join(tokenizer(x,stem_type=stem_type)))
    data_samples.index = range(df.shape[0])
    index_series = df[index_col]
    index_series.index = range(df.shape[0])
    return data_samples,index_series

def print_top_words(model, feature_names, n_top_words):
    '''
    :param model:
    :param feature_names:
    :param n_top_words:
    :return:
    '''
    for topic_idx, topic in enumerate(model.components_):
        print("Topic #%d:" % topic_idx)
        print(" ".join([feature_names[i]
                        for i in topic.argsort()[:-n_top_words - 1:-1]]))
    print()

def get_top_words_df(model, feature_names, n_top_words):
    '''
    :param model:
    :param feature_names:
    :param n_top_words:
    :return:
    '''
    topic_list = []
    for topic_idx, topic in enumerate(model.components_):
        topic_list.append(("Topic {}".format(topic_idx),
                           " ".join([feature_names[i] for i in topic.argsort()[:-n_top_words - 1:-1]]),
                           "_".join([feature_names[i] for i in topic.argsort()[:-4:-1]])))
    return pd.DataFrame.from_records(topic_list,columns=['topic_id','top_words','default_topic_name'])

def get_topics(data_samples,n_topics,n_top_words,max_df=0.9,min_df=0.01):
    '''this function will take a data sample and create topic objects and dataframes
    :param data_samples:
    :param n_topics:
    :param n_top_words:
    :return:
    '''
    tfidf_vectorizer = TfidfVectorizer(max_df=max_df, min_df=min_df)
    tfidf = tfidf_vectorizer.fit_transform(data_samples)
    nmf = NMF(n_components=n_topics, random_state=1, alpha=.1, l1_ratio=.5)
    nmf.fit(tfidf)
    tfidf_feature_names = tfidf_vectorizer.get_feature_names()
    nmf_df = get_top_words_df(nmf, tfidf_feature_names, n_top_words)
    tf_vectorizer = CountVectorizer(max_df=max_df, min_df=min_df)
    tf = tf_vectorizer.fit_transform(data_samples)
    lda = LatentDirichletAllocation(n_topics=n_topics, max_iter=5,
                                learning_method='online', learning_offset=50.,
                                random_state=0)
    lda.fit(tf)
    tf_feature_names = tf_vectorizer.get_feature_names()
    lda_df = get_top_words_df(lda, tf_feature_names, n_top_words)
    return nmf_df,lda_df,nmf,lda,tfidf,tf,tf_feature_names,tfidf_feature_names

def gen_topics_for_input(model,matrix,topics_df,preds_probs_cutoff):
    '''generate the topics predicted by the model
    :param model:
    :param matrix:
    :param topics_df:
    :param preds_probs_cutoff:
    :return:
    '''
    preds = model.transform(matrix)
    preds_probs = preds.transpose()/np.sum(preds,1)
    preds_probs = preds_probs.transpose()
    preds_ind = np.where(preds_probs>preds_probs_cutoff)
    preds_df = pd.DataFrame({'row':preds_ind[0],'col':preds_ind[1]})
    # take the predicted topic id and get corresponding topic name
    pred_topics = pd.merge(preds_df,topics_df,how='left',left_on='col',right_on='topic_id').groupby('row')['topic_name'].\
                            apply(lambda x: '|'.join([i for i in x.tolist() if i !='no_topic'])).head()
    return pred_topics

def get_topics_tagged_df(data_samples,nmf,lda,tf,tfidf,topics_tags_loc):
    '''
    :param df:
    :param topics_tags_loc:topics tagged to name 'no_topic' rows are ignored
    :return:
    '''
    data_samples.index = range(data_samples.shape[0])
    nmf_topics_df = pd.read_excel(topics_tags_loc,sheetname=nmf_topics_sheetname)
    lda_topics_df = pd.read_excel(topics_tags_loc,sheetname=lda_topics_sheetname)
    nmf_topic_names = gen_topics_for_input(nmf,tfidf,nmf_topics_df,0.1)
    lda_topic_names = gen_topics_for_input(lda,tf,lda_topics_df,0.1)
    topic_names_df = pd.concat([data_samples,nmf_topic_names,lda_topic_names],axis=1)#need to correct this format after checking with manual team
    topic_names_df.columns = ['text','nmf_topics','lda_topics']
    return topic_names_df

def save_topic_names_to_file(inp_data_loc,out_loc,text_cols = ('description1','description2'),index_col='ind',
                             stem_type='lemmatize',n_topics=50,n_top_words=20,max_df=0.9,min_df=0.01):
    ''' this function will generate topics as excel file and topic extraction model files as pkl file. The excel files
     need to be manually verified to set topic names for each topic generated by looking at the top words in the topic.
     if the top words are not making sense, set topic name as "no_topic". This should be safved in a column as topic_name.
     After this the code should be run to generate the predictions by the topic model for data provided using these
     topic names, which can be verified manually to measure effectiveness.
    :param inp_data_loc:
    :param out_loc:
    :param text_cols:
    :param index_col:
    :param stem_type:
    :param n_topics:
    :param n_top_words:
    :param max_df:
    :param min_df:
    :return:
    '''
    if out_loc[-1] != '/':
        out_loc = out_loc+'/'
    # get a pandas series object with processed text data and the index column as another series
    data_samples,index_series = get_processed_df(inp_file_loc=inp_data_loc,text_cols=text_cols,stem_type=stem_type)
    # now generate the topics.
    nmf_topics_df,lda_topics_df,nmf,lda,tfidf,tf,tf_feature_names,tfidf_feature_names = \
        get_topics(data_samples,n_topics,n_top_words,max_df,min_df)
    # save the topics into file. This should be verified manually to get topic names for each topic id
    save_to_excel([nmf_topics_df,lda_topics_df],[nmf_topics_sheetname,lda_topics_sheetname],out_loc+'topics_generated.xls')
    with open(out_loc+'topic_extraction_related_files.pkl','w') as f:
        pickle.dump({'nmf_model':nmf,'lda_model':lda,'tfidf_matrix':tfidf,'tf_matrix':tf,
                     'tf_feature_names':tf_feature_names,'tfidf_feature_names':tfidf_feature_names},f)

