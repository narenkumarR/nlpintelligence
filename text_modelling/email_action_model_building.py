__author__ = 'joswin'
import pandas as pd

# reading the data file
tmp = pd.read_excel('email_text_classes.xls')
tmp = tmp.dropna(subset=['unquoted_part_endremoved'])

tmp.groupby('Email Class1').agg({'Email Class1':len})

tmp['Sentiment Class'] = 'Neutral'
tmp.ix[tmp['Email Class'].isin(['Very much interested and schedule meeting/call','Very much interested']),'Sentiment Class'] = 'Positive'
tmp.ix[tmp['Email Class'].isin(['Do not contact/Remove from list',
                                u'No need now \u2013 contact later in x months / no need till 2016 etc',
                             'No need right now - will contact if requirement comes','Not interested - No scope / fit to explore further',
                             'Not interested because using an alternative']),['Sentiment Class']] = 'Negative'
tmp.ix[tmp['Email Class'].isin(['Positive']),'Sentiment Class'] = 'Positive'
tmp.ix[tmp['Email Class'].isin(['Negative']),'Sentiment Class'] = 'Negative'


# actionable,do not contact, negative , nurture, out of office, rest


###########################################
# actionable : Email Address not in use/ Use some other address,Email spam identifier - need action from sender,
#         Need more details to take decision,Not the right person-Have sent it to the right person - He/She will reach if there is interest,
#           Not the right person - Here's the contact info of the right person or the right person is cc'ed – or name of the person to contact is given,
#       Right guy but no longer there and has given the replacer's contacts,Very much interested,Very much interested and schedule meeting/call
#       Right guy but no longer there,
# nurture : Already contacted,No need now \u2013 contact later in x months / no need till 2016 etc,
#         'No need right now - will contact if requirement comes','Not interested because using an alternative',
#         Not the right person but no alternate contact or help offered
#       Reverse Sales Pitch - The recipient is pitching their service,
#       Slight Interest,Slight Interest/ will contact if interested,Will get back
#
# do not contact: Do not contact/Remove from list

# negative:, 'Not interested - No scope / fit to explore further',company shut down
#
# out of office: Out of office/Vacation - contact someone else,Out of office/Vacation - will reply back/
# rest: Random mail/Automatic reply/Neutral mail,mail chain mails for scheduling
#########################################
tmp['Action Class'] = 'rest'

tmp.ix[tmp['Email Class1'].isin(['Email Address not in use/ Use some other address',
                                 'Email spam identifier - need action from sender','Need more details to take decision',
                                 'Not the right person-Have sent it to the right person - He/She will reach if there is interest',
                                 "Right guy but no longer there and has given the replacer's contacts",
                                 "Very much interested and schedule meeting/call",
                                 "Right guy but no longer there","Actionable"]),
       'Action Class'] = 'Actionable'
tmp.ix[tmp['Email Class1'].isin([u"Already contacted,No need now \u2013 contact later in x months / no need till 2016 etc",
                                 'No need right now - will contact if requirement comes','Not interested because using an alternative',
                                 "Not the right person but no alternate contact or help offered",
                                 "Reverse Sales Pitch - The recipient is pitching their service","Slight Interest",
                                 "Slight Interest/ will contact if interested","Will get back","Nurture",
                                 "Very much interested"]),
        'Action Class'] = 'Nurture'
tmp.ix[tmp['Email Class1'].isin(['Do not contact/Remove from list','DNC']),
        'Action Class'] = 'DNC'
tmp.ix[tmp['Email Class1'].isin(["Negative","Not interested - No scope / fit to explore further","company shut down"]),
        'Action Class'] = 'Negative'
tmp.ix[tmp['Email Class1'].isin(["Out of office/Vacation - contact someone else","Out of office/Vacation - will reply back/"]),
        'Action Class'] = 'Out of Office'


mails = tmp.ix[tmp['first_mail']==1,['unquoted_part_endremoved','Action Class','Sentiment Class']]

# model building
import nltk
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


# models within class
#

from sklearn.naive_bayes import MultinomialNB
from naive_bayes import process_text
from sklearn.feature_extraction.text import CountVectorizer
from sklearn import cross_validation
from sklearn import metrics
import numpy as np

mails1 = mails.ix[(mails['Sentiment Class']=='Negative')&(mails['Action Class']!='Actionable'),:]

## model building process starts from here
text_inp = list(mails1['unquoted_part_endremoved'])
text_dv = list(mails1['Action Class'])
text_inp_short,text_dv_short = [],[]
for i in range(len(text_inp)):
    sents = nltk.sent_tokenize(text_inp[i])
    if len(sents)<3:
        text_inp_short.append(text_inp[i])
        text_dv_short.append(text_dv[i])


# negative -> DNC,Negative,Nurture,rest
vocab1 = pd.read_csv('features_11Aug.csv')['features']
vectorizer = CountVectorizer(vocabulary=vocab1,ngram_range=(1,3))
X_train = vectorizer.fit_transform(process_text.process_textlist(text_inp,stem=True,replace_phr_input=replace_phr_input,
                                         stop_words=stop_words,stop_phrases=stop_phrases,
                                         merge_phr_list_keep_original=merge_phr_list_keep_original,
                                         merge_phr_list_remove_original=merge_phr_list_remove_original,
                                         merge_words_with_next_keep_original=merge_words_with_next_keep_original,
                                         merge_words_with_next_remove_original=merge_words_with_next_remove_original))

rm_feats_inds = np.where(X_train.sum(axis=0)==0)[1]
vocab = [vocab1[i] for i in range(len(vocab1)) if i not in rm_feats_inds]
vectorizer = CountVectorizer(vocabulary=vocab,ngram_range=(1,3))
X_train = vectorizer.fit_transform(process_text.process_textlist(text_inp,stem=True,replace_phr_input=replace_phr_input,
                                         stop_words=stop_words,stop_phrases=stop_phrases,
                                         merge_phr_list_keep_original=merge_phr_list_keep_original,
                                         merge_phr_list_remove_original=merge_phr_list_remove_original,
                                         merge_words_with_next_keep_original=merge_words_with_next_keep_original,
                                         merge_words_with_next_remove_original=merge_words_with_next_remove_original))
model = MultinomialNB()
predicted = cross_validation.cross_val_predict(model, X_train,text_dv, cv=10)
metrics.f1_score(text_dv, predicted,average='macro')# 0.76588080028377326
metrics.f1_score(text_dv,predicted,average='micro') # 0.82188679245283014
metrics.confusion_matrix(text_dv, predicted)

# short
vectorizer = CountVectorizer(vocabulary=vocab1,ngram_range=(1,3))
X_train = vectorizer.fit_transform(process_text.process_textlist(text_inp_short,stem=True,replace_phr_input=replace_phr_input,
                                         stop_words=stop_words,stop_phrases=stop_phrases,
                                         merge_phr_list_keep_original=merge_phr_list_keep_original,
                                         merge_phr_list_remove_original=merge_phr_list_remove_original,
                                         merge_words_with_next_keep_original=merge_words_with_next_keep_original,
                                         merge_words_with_next_remove_original=merge_words_with_next_remove_original))
rm_feats_inds = np.where(X_train.sum(axis=0)==0)[1]
vocab_short = [vocab1[i] for i in range(len(vocab1)) if i not in rm_feats_inds]
vectorizer = CountVectorizer(vocabulary=vocab_short,ngram_range=(1,3))
X_train = vectorizer.fit_transform(process_text.process_textlist(text_inp_short,stem=True,replace_phr_input=replace_phr_input,
                                         stop_words=stop_words,stop_phrases=stop_phrases,
                                         merge_phr_list_keep_original=merge_phr_list_keep_original,
                                         merge_phr_list_remove_original=merge_phr_list_remove_original,
                                         merge_words_with_next_keep_original=merge_words_with_next_keep_original,
                                         merge_words_with_next_remove_original=merge_words_with_next_remove_original))

model = MultinomialNB()
predicted = cross_validation.cross_val_predict(model, X_train,text_dv_short, cv=10)
metrics.f1_score(text_dv_short, predicted,average='macro')# 0.76588080028377326
metrics.f1_score(text_dv_short,predicted,average='micro') # 0.82188679245283014
metrics.confusion_matrix(text_dv_short, predicted)

from naive_bayes.naive_bayes_model import NaiveBayesModel
nb_action_neg = NaiveBayesModel()
nb_action_neg.fit_model(text_inp,text_dv,stem=True,replace_phr_input=replace_phr_input,
             stop_words=stop_words,stop_phrases=stop_phrases,merge_phr_list_keep_original=merge_phr_list_keep_original,
             merge_phr_list_remove_original=merge_phr_list_remove_original,merge_words_with_next_keep_original=merge_words_with_next_keep_original,
             merge_words_with_next_remove_original=merge_words_with_next_remove_original,stop_words_vectorizer=stop_words_vectorizer,
             feature_loc=vocab,n_gram_range=(1,3),random_state=10
         )
nb_action_neg_short = NaiveBayesModel()
nb_action_neg_short.fit_model(text_inp_short,text_dv_short,stem=True,replace_phr_input=replace_phr_input,
             stop_words=stop_words,stop_phrases=stop_phrases,merge_phr_list_keep_original=merge_phr_list_keep_original,
             merge_phr_list_remove_original=merge_phr_list_remove_original,merge_words_with_next_keep_original=merge_words_with_next_keep_original,
             merge_words_with_next_remove_original=merge_words_with_next_remove_original,stop_words_vectorizer=stop_words_vectorizer,
             feature_loc=vocab_short,n_gram_range=(1,3),random_state=10
         )

#positive
mails1 = mails.ix[(mails['Sentiment Class']=='Positive'),:]
# there is not enough data to do this prediction. put all positive as actionable

# neutral (negative in neutral, not enough data. remove them. if neutral, will not go to negative
mails1 = mails.ix[(mails['Sentiment Class']=='Neutral')&(mails['Action Class']!='Negative'),:]
# run above codes to determine the features
nb_action_neutral = NaiveBayesModel()
nb_action_neutral.fit_model(text_inp,text_dv,stem=True,replace_phr_input=replace_phr_input,
             stop_words=stop_words,stop_phrases=stop_phrases,merge_phr_list_keep_original=merge_phr_list_keep_original,
             merge_phr_list_remove_original=merge_phr_list_remove_original,merge_words_with_next_keep_original=merge_words_with_next_keep_original,
             merge_words_with_next_remove_original=merge_words_with_next_remove_original,stop_words_vectorizer=stop_words_vectorizer,
             feature_loc=vocab,n_gram_range=(1,3),random_state=10
         )
nb_action_neutral_short = NaiveBayesModel()
nb_action_neutral_short.fit_model(text_inp_short,text_dv_short,stem=True,replace_phr_input=replace_phr_input,
             stop_words=stop_words,stop_phrases=stop_phrases,merge_phr_list_keep_original=merge_phr_list_keep_original,
             merge_phr_list_remove_original=merge_phr_list_remove_original,merge_words_with_next_keep_original=merge_words_with_next_keep_original,
             merge_words_with_next_remove_original=merge_words_with_next_remove_original,stop_words_vectorizer=stop_words_vectorizer,
             feature_loc=vocab_short,n_gram_range=(1,3),random_state=10
         )

