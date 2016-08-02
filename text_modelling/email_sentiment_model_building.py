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
    ,'which','who','out','now', u'you', u'your', u'yours','more']
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
