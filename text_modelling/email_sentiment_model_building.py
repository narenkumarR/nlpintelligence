#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'joswin'
import pandas as pd

tmp = pd.read_excel('/home/madan/Desktop/joswin_bck/toPendrive/works/nlp-intelligence/shrikanth_docs/email_text_classes.xls')
# tmp = pd.read_excel('/home/madan/Desktop/joswin_bck/toPendrive/works/pipecandy_bck/shrikanth_docs/Email_classification.xls')
tmp = tmp.dropna(subset=['unquoted_part_endremoved'])
tmp.groupby('Email Class').agg({'Email Class':len})

# Interested, Interested, schedule meeting -> interested
# Not interested,do not contact -> not interested
# need details,out of office -> not sure
# random,mail chain -> other

# Interested :Very much interested, Very much interested and schedule meeting/call,
# Neutral : Already contacted,Email Address not in use/ Use some other address,Email spam identifier - need action from sender,
#             Not the right person ,Out of office/Vacation,Random mail/Automatic reply/Neutral mail,Reverse Sales Pitch
#             Right guy but no longer there,company shut down, mail chain mails,Need more details to take decision,
#           Slight Interest/ will contact if interested
# Not interested : Do not contact/Remove from list,No need now ,Not interested - No scope,Not interested because using an alternative

# creating sentiment column
tmp['Sentiment Class'] = 'Neutral'
tmp.ix[tmp['Email Class'].isin(['Very much interested and schedule meeting/call','Very much interested']),'Sentiment Class'] = 'Positive'
tmp.ix[tmp['Email Class'].isin(['Do not contact/Remove from list',
                                u'No need now \u2013 contact later in x months / no need till 2016 etc',
                             'No need right now - will contact if requirement comes','Not interested - No scope / fit to explore further',
                             'Not interested because using an alternative']),['Sentiment Class']] = 'Negative'

# only selecting first mails
mails = tmp.ix[tmp['first_mail']==1,['unquoted_part_endremoved','Sentiment Class']]



###################################################################
############################first level model
from naive_bayes.naive_bayes_model import NaiveBayesModel
replace_phr_input = {'not': 'no',"n't":'no','dont':'no','wont':'no','cant':'no','non':'no','your':'you'
                                                    ,'yours':'you'}
stop_phrases = ["curated essays","enterprise tech"]
merge_phr_list_remove_original = []
merge_phr_list_keep_original = []
merge_words_with_next_remove_original = ['no']
merge_words_with_next_keep_original = ['stop']
from nltk.corpus import stopwords
stop_words=stopwords.words('english')
stops_add_list = [u'!', u"'", u"'d", u"'ll", u"'m", u"'re", u"'s", u"'ve", u',', u'-','_','www','facebook','ashwin'
    ,'youtube','linkedin','twitter','http','contractiq','techcrunch','suriyah','suriyahk','krishnan','https','hey'
    ,'iphon','hello','hi','chuck','really','probably','regards','founder','co','cofounder','de','cheers','please','been'
    ]
stops_del_list = ['not','no','take','off','appreciate','non','look','need','believe','see','','what','when','where','why','how'
    ,'which','who','out','now', u'you', u'your', u'yours','more']
stop_words = list(set(stop_words+stops_add_list))
stop_words = list(set(stop_words)-set(stops_del_list))
stop_words_vectorizer = list(set(stop_words+['thank','iphon','hello','hi','chuck','you','in']))

nb = NaiveBayesModel()
nb.fit_model(list(mails['unquoted_part_endremoved']),list(mails['Sentiment Class']),stem=True,replace_phr_input=replace_phr_input,
             stop_words=stop_words,stop_phrases=stop_phrases,merge_phr_list_keep_original=merge_phr_list_keep_original,
             merge_phr_list_remove_original=merge_phr_list_remove_original,merge_words_with_next_keep_original=merge_words_with_next_keep_original,
             merge_words_with_next_remove_original=merge_words_with_next_remove_original,stop_words_vectorizer=stop_words_vectorizer,
             feature_loc=None,default_class='Neutral'
             )

# measuring accuracy

# stemming is done
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

# try without stemming
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
