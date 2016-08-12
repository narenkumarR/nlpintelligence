#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'joswin'
import pandas as pd

# reading the data file
tmp = pd.read_excel('email_text_classes.xls')
tmp = tmp.dropna(subset=['unquoted_part_endremoved'])

tmp.groupby('Email Class').agg({'Email Class':len})


#####################################################################
############## creating sentiment column #########################################
############# merging different Email Class into three major classes: Positive, Neutral and Negative#######

# Positive :Very much interested, Very much interested and schedule meeting/call,
# Neutral : Already contacted,Email Address not in use/ Use some other address,Email spam identifier - need action from sender,
#             Not the right person ,Out of office/Vacation,Random mail/Automatic reply/Neutral mail,Reverse Sales Pitch
#             Right guy but no longer there,company shut down, mail chain mails,Need more details to take decision,
#           Slight Interest/ will contact if interested
# Negative : Do not contact/Remove from list,No need now ,Not interested - No scope,Not interested because using an alternative


tmp['Sentiment Class'] = 'Neutral'
tmp.ix[tmp['Email Class'].isin(['Very much interested and schedule meeting/call','Very much interested']),'Sentiment Class'] = 'Positive'
tmp.ix[tmp['Email Class'].isin(['Do not contact/Remove from list',
                                u'No need now \u2013 contact later in x months / no need till 2016 etc',
                             'No need right now - will contact if requirement comes','Not interested - No scope / fit to explore further',
                             'Not interested because using an alternative']),['Sentiment Class']] = 'Negative'
tmp.ix[tmp['Email Class'].isin(['Positive']),'Sentiment Class'] = 'Positive'
tmp.ix[tmp['Email Class'].isin(['Negative']),'Sentiment Class'] = 'Negative'
# tmp.ix[tmp['Email Class'].isin(['Positive'],'Sentiment Class'] = 'Positive'

# only selecting first mails 
mails = tmp.ix[tmp['first_mail']==1,['unquoted_part_endremoved','Sentiment Class']]



###################################################################
############################first level model #######################
#################### 3 classes : Positive, Neutral and Negative ##########

###### Pre-processing ######

#### Stopwords #####
from nltk.corpus import stopwords
stop_words=stopwords.words('english')
## the following words are added to the stopwords list
stops_add_list = [u'!', u"'", u"'d", u"'ll", u"'m", u"'re", u"'s", u"'ve", u',', u'-','_','www','facebook','ashwin'
    ,'youtube','linkedin','twitter','http','contractiq','techcrunch','suriyah','suriyahk','krishnan','https','hey'
    ,'iphon','hello','hi','chuck','really','probably','regards','founder','co','cofounder','de','cheers','please','been'
    ]
## the following words are removed from the stopwords list
stops_del_list = ['not','no','take','off','appreciate','non','look','need','believe','see','','what','when','where','why','how'
    ,'which','who','out','now', u'you', u'your', u'yours']
stop_words = list(set(stop_words+stops_add_list))
stop_words = list(set(stop_words)-set(stops_del_list))

# these words also added to the stopwords list
stop_words_vectorizer = list(set(stop_words+['thank','iphon','hello','hi','chuck','you','in']))

# these phrases also are removed in the final data.
stop_phrases = ["curated essays","enterprise tech"]

##################################
###### Creating phrases ##########
##################################

## This dictionary is used for replacing the phrases in the data. 
# {key:value} # key will be replace with value
# eg: in the text, 'not' will be replaced with 'no'
replace_phr_input = {'not': 'no',"n't":'no','dont':'no','wont':'no','cant':'no','non':'no','your':'you'
                                                    ,'yours':'you'}

## list of phrases which can be merged (the original words in the phrase will be removed) 
## eg: 'no interest' -> will be converted to 'no_interest'
merge_phr_list_remove_original = []

## list of phrases which can be merged (the original words will be kept) 
## eg: 'no interest' -> will be converted to 'no_interest no interest'
merge_phr_list_keep_original = []

## list words which will be merged with the next word to create a phrase (original workds will be removed)
## eg: 'no need','no chance' etc -> will be converted to 'no_need','no_chance' etc
merge_words_with_next_remove_original = ['no']

## list words which will be merged with the next word to create a phrase (original workds will be kept)
## eg: 'stop send','stop spam' etc -> will be converted to 'stop_send stop send','stop_spam stop spam' etc
merge_words_with_next_keep_original = ['stop']

from naive_bayes.naive_bayes_model import NaiveBayesModel
nb = NaiveBayesModel()
# fitting the naive bayes model
nb.fit_model(list(mails['unquoted_part_endremoved']),list(mails['Sentiment Class']),stem=True,replace_phr_input=replace_phr_input,
             stop_words=stop_words,stop_phrases=stop_phrases,merge_phr_list_keep_original=merge_phr_list_keep_original,
             merge_phr_list_remove_original=merge_phr_list_remove_original,merge_words_with_next_keep_original=merge_words_with_next_keep_original,
             merge_words_with_next_remove_original=merge_words_with_next_remove_original,stop_words_vectorizer=stop_words_vectorizer,
             feature_loc=None,default_class='Neutral'
             )

################################################
################ measuring accuracy ############
################################################

##############################################
############## stemming is done ##############
##############################################
X_train = nb._get_dtm_textlist(list(mails['unquoted_part_endremoved']))
y_train = pd.Series(list(mails['Sentiment Class']))
from sklearn.naive_bayes import MultinomialNB
from sklearn import cross_validation
model = MultinomialNB(class_prior=nb.class_wts)
predicted = cross_validation.cross_val_predict(model, X_train,y_train, cv=10)

from sklearn import metrics
metrics.f1_score(y_train, predicted,average='macro')# 0.76121695671844203
metrics.f1_score(y_train,predicted,average='micro') # 0.81962264150943398
metrics.confusion_matrix(y_train, predicted)
# array([[719,  50,  33],
#        [ 66, 239,  69],
#        [  7,  14, 128]])

#########################################
######### try without stemming ##########
#########################################
X_train = nb._get_dtm_textlist(list(mails['unquoted_part_endremoved']))
y_train = pd.Series(list(mails['Sentiment Class']))
from sklearn.naive_bayes import MultinomialNB
from sklearn import cross_validation
model = MultinomialNB(class_prior=nb.class_wts)
predicted = cross_validation.cross_val_predict(model, X_train,y_train, cv=10)

from sklearn import metrics
metrics.f1_score(y_train, predicted,average='macro') # 0.75745783187951077
metrics.f1_score(y_train,predicted,average='micro') # 0.81509433962264155
metrics.confusion_matrix(y_train, predicted)
# array([[720,  50,  32],
#        [ 74, 228,  72],
#        [  6,  11, 132]])

# final model is the stemmed version
import cPickle as pickle
with open('first_level_3class_model.pkl','w') as f:
    pickle.dump(nb,f)

# saving features
nb.save_feature_names('first_level_3class_features.txt')

# Second level models
mails = tmp.ix[tmp['first_mail']==1,['unquoted_part_endremoved','Sentiment Class','Email Class']]

# second level positive
pos_mails = mails.ix[mails['Sentiment Class'] == 'Positive',:]
pos_mails.groupby('Email Class').agg({'Email Class':len}) # only 11 with

# second level negative
neg_mails = mails.ix[mails['Sentiment Class'] == 'Negative',:]
neg_mails.groupby('Email Class').agg({'Email Class':len}) # only 11 with



######################################################
############### Works from 8 August ##################
######################################################

# add timex to determine time in text
from text_processing import timex
nb = NaiveBayesModel()
inp_list = list(mails['unquoted_part_endremoved'])
class_list = list(mails['Sentiment Class'])
inp_list = [txt+' datetime_in_text_xxx' if timex.check_if_date_present(txt) else txt for txt in inp_list]

merge_words_with_next_remove_original = ['no','stop']
merge_words_with_next_keep_original = []

nb.fit_model(inp_list,class_list,stem=True,replace_phr_input=replace_phr_input,
             stop_words=stop_words,stop_phrases=stop_phrases,merge_phr_list_keep_original=merge_phr_list_keep_original,
             merge_phr_list_remove_original=merge_phr_list_remove_original,merge_words_with_next_keep_original=merge_words_with_next_keep_original,
             merge_words_with_next_remove_original=merge_words_with_next_remove_original,stop_words_vectorizer=stop_words_vectorizer,
             feature_loc=None,default_class='Neutral'
             )

X_train = nb._get_dtm_textlist(inp_list)
y_train = pd.Series(class_list)
from sklearn.naive_bayes import MultinomialNB
from sklearn import cross_validation
model = MultinomialNB(class_prior=nb.class_wts)
predicted = cross_validation.cross_val_predict(model, X_train,y_train, cv=10)

from sklearn import metrics
metrics.f1_score(y_train, predicted,average='macro')# 0.76588080028377326
metrics.f1_score(y_train,predicted,average='micro') # 0.82188679245283014
metrics.confusion_matrix(y_train, predicted)
# array([[719,  50,  33],
#        [ 67, 240,  67],
#        [  7,  12, 130]])


###### New approach.. build the feature set from what ashwin provided. then use it on the mails data
from sklearn.feature_extraction.text import CountVectorizer
from text_processing import extract_phrases
from text_processing.word_transformations import Tokenizer
tk = Tokenizer()

indf = pd.read_csv('/home/madan/Desktop/joswin_bck/toPendrive/works/nlp-intelligence/email_classification/8Aug_works/Email Responses.csv')
vectorizer = CountVectorizer(min_df=1)
text_list = [extract_phrases.multiple_replace(replace_phr_input, ' '.join(nltk.word_tokenize(text.lower())),word_limit=True,flags=2) for text in text_list]
text_list = tk.stopword_removal_listinput(text_list,stop_words)
text_list = tk.stop_phrase_removal_listinput(text_list,[wrd for wrd in stop_words if len(wrd)>1])
text_list = tk.stop_phrase_removal_listinput(text_list,stop_phrases)
X_train = vectorizer.fit_transform(text_list)
vocab = vectorizer.get_feature_names()
with open('/home/madan/Desktop/joswin_bck/toPendrive/works/nlp-intelligence/email_classification/8Aug_works/features_ashwin.txt','w') as f:
     for i in vocab:
         f.write(i+'\n')


# feature building
# chunking
from nltk.corpus import stopwords
from nltk.corpus import stopwords
stop_words=stopwords.words('english')
## the following words are added to the stopwords list
stops_add_list = [u'!', u"'", u"'d", u"'ll", u"'m", u"'re", u"'s", u"'ve", u',', u'-','_','www','facebook','ashwin'
    ,'youtube','linkedin','twitter','http','contractiq','techcrunch','suriyah','suriyahk','krishnan','https','hey'
    ,'iphon','hello','hi','chuck','really','probably','regards','founder','co','cofounder','de','cheers','please','been',
                  u'you', u'your', u'yours'
    ]
## the following words are removed from the stopwords list
stops_del_list = ['not','no','take','off','appreciate','non','look','need','believe','see','','what','when','where','why','how'
    ,'which','who','out','now']
stop_words = list(set(stop_words+stops_add_list))
stop_words = list(set(stop_words)-set(stops_del_list))

# these phrases also are removed in the final data.
stop_phrases = ["curated essays","enterprise tech"]
# these words also added to the stopwords list
stop_words_vectorizer = list(set(stop_words+['thank','iphon','hello','hi','chuck','you','in','thank']))

replace_phr_input = {'not': 'no',"n't":'no','dont':'no','wont':'no','cant':'no','non':'no','your':'you'
                            ,'yours':'you','e-mail':'email',
                     'shouldnt':'should not','couldnt':'could not'
                     ,'monday':'mon','tuesday':'tue','wednesday':'wed',
                     'thursday':'thu','friday':'fri','saturday':'sat'
                    }
import nltk,re
grammar = r"""
    P2: {<RB>*<VB.*|JJ>}         # not interested, no need
    P3: {<PR.+>*<RP|IN>}        #me off
    P4: {<PR.+><NN.*>}          # your list
    P7: {<P2><P3|NN.*|PR.+>}         #Take me off, good idea,Tell me, right person
    P8: {<P7><CC>}         #good idea but
    """
cp = nltk.RegexpParser(grammar)

new_sample = pd.read_csv('Email Responses.csv')

text_list = list(mails['unquoted_part_endremoved'])+list(new_sample['Response Content'])
text_list1 = [cp.parse(nltk.pos_tag(nltk.word_tokenize(i))) for i in text_list]
from text_processing import extract_phrases
from text_processing.word_transformations import Tokenizer
tk = Tokenizer()

pe = extract_phrases.PhraseExtractor()
phr_list = []
for tr in text_list1:
    for phr in pe.extract_phrase_treeinput(tr,['P2','P3','P4','P7','P8']):
        phr = extract_phrases.multiple_replace(replace_phr_input, ' '.join(nltk.word_tokenize(phr.lower())),word_limit=True,flags=2)
        phr = tk.stopword_removal_textinput(phr,stop_words)
        phr = tk.stop_phrase_removal_textinput(phr,re.compile('\\b|\\b'.join(stop_phrases)))
        phr = tk.stop_phrase_removal_textinput(phr,re.compile('\\b|\\b'.join([wrd for wrd in stop_words if len(wrd)>1])))
        phr = tk.snowball_stemmer_textinput(phr)
        if phr and len(phr.split(' '))>1:
            phr_list.append(phr)

pd.Series(phr_list).value_counts().to_csv('phrases_freq.csv')

# generate single words
grammar = r"""
    P1: {<JJ.*>}
    P2: {<VB.*>}
    P3: {<NN|NNS>}
    P4: {<RB.*>}
    """
cp = nltk.RegexpParser(grammar)
text_list1 = [cp.parse(nltk.pos_tag(nltk.word_tokenize(i))) for i in text_list]
wrd_list = []
for tr in text_list1:
    for phr in pe.extract_phrase_treeinput(tr,['P1','P2','P3','P4']):
        phr = extract_phrases.multiple_replace(replace_phr_input, ' '.join(nltk.word_tokenize(phr.lower())),word_limit=True,flags=2)
        phr = tk.stopword_removal_textinput(phr,stop_words_vectorizer)
        phr = tk.stop_phrase_removal_textinput(phr,re.compile('\\b|\\b'.join(stop_phrases)))
        phr = tk.stop_phrase_removal_textinput(phr,re.compile('\\b|\\b'.join([wrd for wrd in stop_words if len(wrd)>1])))
        phr = tk.snowball_stemmer_textinput(phr)
        phr = tk.stopword_removal_textinput(phr,stop_words_vectorizer)
        for wrd in phr.split():
            if wrd and len(wrd)>1:
                # if wrd == 'cid':
                #     print tr
                wrd_list.append(wrd)

pd.Series(wrd_list).value_counts().to_csv('wrds_freq.csv')

# merging all features
final_feat = []
sheet_names = ['ashwin_phr','mails_phr','ashwin_wrds','mails_final']
for sheet in sheet_names:
    tmp = pd.read_excel('final_features.xls',sheetname = sheet,header=None)
    final_feat.extend(list(tmp[0]))

final_feat = list(set(final_feat))
final_feat.sort()
tmp = pd.DataFrame({'features':final_feat})
tmp.to_csv('features_9Aug.csv',index=False)

### model building
from nltk.corpus import stopwords
stop_words=stopwords.words('english')
## the following words are added to the stopwords list
stops_add_list = [u'!', u"'", u"'d", u"'ll", u"'m", u"'re", u"'s", u"'ve", u',', u'-','_','www','facebook','ashwin'
    ,'youtube','linkedin','twitter','http','contractiq','techcrunch','suriyah','suriyahk','krishnan','https','hey'
    ,'iphon','hello','hi','chuck','really','probably','regards','founder','co','cofounder','de','cheers','please','been',
                  u'you', u'your', u'yours'
    ]
## the following words are removed from the stopwords list
stops_del_list = ['not','no','take','off','appreciate','non','look','need','believe','see','','what','when','where','why','how'
    ,'which','who','out','now','me','again']
stop_words = list(set(stop_words+stops_add_list))
stop_words = list(set(stop_words)-set(stops_del_list))

# these phrases also are removed in the final data.
stop_phrases = ["curated essays","enterprise tech"]
# these words also added to the stopwords list
stop_words_vectorizer = list(set(stop_words+['thank','iphon','hello','hi','chuck','you','in','thank','me']))

replace_phr_input = {'not': 'no',"n't":'no','non':'no','your':'you'
                            ,'yours':'you','e-mail':'mail','email':'mail','emails':'mail','nope':'no'
                     ,'monday':'mon','tuesday':'tue','wednesday':'wed',
                     'thursday':'thu','friday':'fri','saturday':'sat'
#                     }
                        ,
# replace_phr_input_1 = {
                    'shouldnt':'shouldntt no',"shouldn't":'shouldntt no','should not':'shouldntt no',
                       "don't":'dontt no','dont':'dontt no','do not':'dontt no',
                       'wont':'willntt no',"willn't":'willntt no','will not':'willntt no',
                       'cant':'cantt no',"can't":'cantt no','can not':'cantt no'
                       }
merge_phr_list_remove_original = []
merge_phr_list_keep_original = []
merge_words_with_next_remove_original = ['no']
merge_words_with_next_keep_original = []

# data
# find technologies in text
import re
# techs_reg = re.compile(r"java|python|node js|nodejs|angular js|angualrjs|reactjs|react js|\.net\b",
#                        re.IGNORECASE)
# text_inp = [txt+' tech_in_text_xxx' if techs_reg.search(txt) else txt for txt in text_inp]

# fitting the naive bayes model
text_inp = list(mails['unquoted_part_endremoved'])
text_dv = list(mails['Sentiment Class'])

from naive_bayes.naive_bayes_model import NaiveBayesModel
nb = NaiveBayesModel()
nb.fit_model(text_inp,text_dv,stem=True,replace_phr_input=replace_phr_input,
             stop_words=stop_words,stop_phrases=stop_phrases,merge_phr_list_keep_original=merge_phr_list_keep_original,
             merge_phr_list_remove_original=merge_phr_list_remove_original,merge_words_with_next_keep_original=merge_words_with_next_keep_original,
             merge_words_with_next_remove_original=merge_words_with_next_remove_original,stop_words_vectorizer=stop_words_vectorizer,
             feature_loc='features_9Aug.csv',default_class='Neutral',n_gram_range=(1,3),random_state=10
         )

# short sentence separate
text_inp_short,text_dv_short = [],[]
import nltk
for i in range(len(text_inp)):
    sents = nltk.sent_tokenize(text_inp[i])
    if len(sents)<3:
        text_inp_short.append(text_inp[i])
        text_dv_short.append(text_dv[i])

# nearest neighbor
from naive_bayes import process_text
from sklearn.feature_extraction.text import CountVectorizer
from sklearn import cross_validation
from sklearn import metrics

vocab = pd.read_csv('features_9Aug.csv')['features']
vectorizer = CountVectorizer(vocabulary=vocab,ngram_range=(1,3))
X_train = vectorizer.fit_transform(process_text.process_textlist(text_inp,stem=True,replace_phr_input=replace_phr_input,
                                         stop_words=stop_words,stop_phrases=stop_phrases,
                                         merge_phr_list_keep_original=merge_phr_list_keep_original,
                                         merge_phr_list_remove_original=merge_phr_list_remove_original,
                                         merge_words_with_next_keep_original=merge_words_with_next_keep_original,
                                         merge_words_with_next_remove_original=merge_words_with_next_remove_original))
from sklearn.neighbors import KNeighborsClassifier
neigh = KNeighborsClassifier(n_neighbors=5)
neigh.fit(X_train,text_dv)

neigh = KNeighborsClassifier(n_neighbors=5)
predicted = cross_validation.cross_val_predict(neigh, X_train,text_dv, cv=10)
metrics.f1_score(text_dv, predicted,average='macro')# 0.76588080028377326
metrics.f1_score(text_dv,predicted,average='micro') # 0.82188679245283014
metrics.confusion_matrix(text_dv, predicted)

# svm
from sklearn.svm import SVC
clf = SVC()
clf.fit(X_train,text_dv)

clf = SVC(C=10,class_weight='balanced')
predicted = cross_validation.cross_val_predict(clf, X_train,text_dv, cv=10)
metrics.f1_score(text_dv, predicted,average='macro')# 0.76588080028377326
metrics.f1_score(text_dv,predicted,average='micro') # 0.82188679245283014
metrics.confusion_matrix(text_dv, predicted)

# random forest
from sklearn.ensemble import RandomForestClassifier
rf = RandomForestClassifier(n_estimators=100,min_samples_split=5)
predicted = cross_validation.cross_val_predict(rf, X_train,text_dv, cv=10)

from sklearn.naive_bayes import MultinomialNB
model = MultinomialNB(class_prior=nb.class_wts)
predicted = cross_validation.cross_val_predict(model, X_train,text_dv, cv=10)


# final model as on 11 Aug for release
# create X_train using vocabulary in 'features_9Aug.csv'
# first remove fields which are not present in X_train
import numpy as np
rm_list = []
zero_wrd_inds = np.where(X_train.sum(axis=0)==0)[1]
for i in zero_wrd_inds:
    rm_list.append(vocab[i])

# look at wrds <10, look at their predictions and if doesn't make sense remove
inds = set(np.where(X_train.sum(axis=0)<=10)[1])-set(zero_wrd_inds)
lt10_wrds = []
for i in inds:
    lt10_wrds.append(vocab[i])
    print vocab[i],nb.predict_textinput(vocab[i],1)

rm_list.extend(list(set(lt10_wrds)-set(['abl','believ','bit','come','confirm','contact list','curiou',
                                        'detail document','ever','explor','how tue','learn','let talk',
                                        'mail access','next fri','next month','no_hesit','no_now','no_plan',
                                        'no_requir','no_right person','no_know','no_understand','outsid','off list',
                                        'remov us','return fri','return mon','return tue','return wed',
                                        'right person','slot','sound interest','speak tomorrow','stop spam',
                                        'thu morn','touch base','tri out','what time','sat'])))

new_vocab = list(set(vocab)-set(rm_list))
new_vocab.sort()
pd.DataFrame({'features':new_vocab}).to_csv('features_11Aug.csv',index=False)

new_vocab = pd.read_csv('features_11Aug.csv')['features']
vectorizer = CountVectorizer(vocabulary=new_vocab,ngram_range=(1,3))
text_inp = list(mails['unquoted_part_endremoved'])
text_dv = list(mails['Sentiment Class'])
X_train = vectorizer.fit_transform(process_text.process_textlist(text_inp,stem=True,replace_phr_input=replace_phr_input,
                                         stop_words=stop_words,stop_phrases=stop_phrases,
                                         merge_phr_list_keep_original=merge_phr_list_keep_original,
                                         merge_phr_list_remove_original=merge_phr_list_remove_original,
                                         merge_words_with_next_keep_original=merge_words_with_next_keep_original,
                                         merge_words_with_next_remove_original=merge_words_with_next_remove_original))


model = MultinomialNB(class_prior=nb.class_wts)
predicted = cross_validation.cross_val_predict(model, X_train,text_dv, cv=10)
# accuracy little lower. but still putting it in production(along with settu sirs model. ensembling need to be decided)
# fit naive bayes using code above (both full and short)
from naive_bayes.naive_bayes_model import NaiveBayesModel
nb = NaiveBayesModel()
nb.fit_model(text_inp,text_dv,stem=True,replace_phr_input=replace_phr_input,
             stop_words=stop_words,stop_phrases=stop_phrases,merge_phr_list_keep_original=merge_phr_list_keep_original,
             merge_phr_list_remove_original=merge_phr_list_remove_original,merge_words_with_next_keep_original=merge_words_with_next_keep_original,
             merge_words_with_next_remove_original=merge_words_with_next_remove_original,stop_words_vectorizer=stop_words_vectorizer,
             feature_loc='features_11Aug.csv',default_class='Neutral',n_gram_range=(1,3),random_state=10
         )
with open('naive_bayes_11Aug_full.pkl','w') as f:
  pickle.dump(nb,f)

text_inp_short,text_dv_short= [],[]
for i in range(len(text_inp)):
    if len(nltk.sent_tokenize(text_inp[i])) <= 3:
        text_inp_short.append(text_inp[i])
        text_dv_short.append(text_dv[i])

nb_short =  NaiveBayesModel()
nb_short.fit_model(text_inp_short,text_dv_short,stem=True,replace_phr_input=replace_phr_input,
             stop_words=stop_words,stop_phrases=stop_phrases,merge_phr_list_keep_original=merge_phr_list_keep_original,
             merge_phr_list_remove_original=merge_phr_list_remove_original,merge_words_with_next_keep_original=merge_words_with_next_keep_original,
             merge_words_with_next_remove_original=merge_words_with_next_remove_original,stop_words_vectorizer=stop_words_vectorizer,
             feature_loc='features_11Aug.csv',default_class='Neutral',n_gram_range=(1,3),random_state=10
         )
