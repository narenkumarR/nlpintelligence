__author__ = 'joswin'

import pandas as pd
import re
import nltk

r_stopwords = ["i","me","my","myself","we","our","ours","ourselves","you","your","yours","yourself","yourselves",
               "he","him","his","himself","she","her","hers","herself","it","its","itself","they","them","their",
               "theirs","themselves","what","which","who","whom","this","that","these","those","am","is","are","was",
               "were","be","been","being","have","has","had","having","do","does","did","doing","would","should",
               "could","ought","i'm","you're","he's","she's","it's","we're","they're","i've","you've","we've",
               "they've","i'd","you'd","he'd","she'd","we'd","they'd","i'll","you'll","he'll","she'll","we'll",
               "they'll","isn't","aren't","wasn't","weren't","hasn't","haven't","hadn't","doesn't","don't",
               "didn't","won't","wouldn't","shan't","shouldn't","can't","cannot","couldn't","mustn't","let's",
               "that's","who's","what's","here's","there's","when's","where's","why's","how's","a","an","the",
               "and","but","if","or","because","as","until","while","of","at","by","for","with","about","against",
               "between","into","through","during","before","after","above","below","to","from","up","down","in",
               "out","on","off","over","under","again","further","then","once","here","there","when","where","why",
               "how","all","any","both","each","few","more","most","other","some","such","no","nor","not","only",
               "own","same","so","than","too","very"]

exceptions = ["not", "do", "no", "in", "me","this","off", "for", "at","don't", "didn't","you","this"]
stopwrds_add = ["hi", "'d", "hey", "suriyah", "i m", "ll", "o", "p",  "m", "co",
                  "pm","re", "s", "ve", "hello", "dear", "mr", "vidya", "th", "it s",
                  "iphone", "founder", "best", "regards", "google", "subscribe","youtube",
                  "link", "follow","twitter","facebook","website","http","www","com","org",
                  "''", "ashwin", "contractiq", "suriya", "since", "mike", "file",
                  "database","vp", "writing", "pls", "inbox", "monday", "tuesday",
                  "wednesday","friday", "ipad", "september", "august","tom", "january",
                  "rd", "click","gmt","st", "linkedin", "uk", "krishnan","india",
                  "original", "message", "day"]
stopwrds_full = list(set(r_stopwords+stopwrds_add)-set(exceptions))

def text_vectors(text):
    '''
    :param text:
    :return:
    '''
    text = re.sub('http\\S+\\s*',' ',text)
    text = re.sub("www\\S+\\s*",' ',text)
    text = re.sub("//\\S+\\s*",' ',text)
    text = re.sub("/",' ',text)
    text = re.sub("-",' ',text)
    text = text.lower()
    text = re.sub("n't",'not',text)
    text = re.sub("n 't",'not',text)
    text = re.sub("can not","cannot",text)
    text = re.sub("e mail","email",text)
    text = re.sub("","",text)
    text = re.sub("don't","donot",text)
    text = re.sub("\\bdev\\b","development",text)
    text = re.sub("chief executive officer","ceo",text)
    text = re.sub("thank\\b","thanks",text)
    text = re.sub("\\bdon t\\b","donot",text)
    text = re.sub("\\bdo not\\b","donot",text)
    text = re.sub("","",text)
    text = re.sub("","",text)
    text = re.sub("","",text)
    text = re.sub("","",text)
    text = re.sub("","",text)
    text = re.sub("","",text)
    text = re.sub("","",text)
    text = ' '.join([wrd for wrd in  nltk.word_tokenize(text) if wrd not in stopwrds_full])
    text = re.sub(r"'",' ',text)
    text = re.sub(r' +',' ',text)
    text = ' '.join(wrd for wrd in text.split(' ') if len(wrd)>1)
    return text

def text_vectors_list(text_list):
    return [text_vectors(text) for text in text_list]

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.ensemble import RandomForestClassifier
import numpy as np

vocabulary = ['y','access','add','additional','address','already','also','anything','app','area','assistance',
              'at','available','back','business','call','can','case','ceo','change','changes','check','cheers',
              'cid','companies','company','confidential','connect','contact','contacting','copy','cto','current',
              'currently','delete','details','developers','development','director','discuss','do','donot','email',
              'emails','engineering','feel','first','fit','for.','forward','forwarded','free','future','get',
              'give','going','good','great','group','happy','help','hope','house','however','immediate','in.',
              'inc.','info','information','inquiry','intended','interest','interested','interesting','just',
              'keep','kind','know','last','let','like','limited','line','list','longer','look','looking','mail',
              'mailing','manager','many','marketing','may','me','meet','mind','mobile','moment','morning','need',
              'needs','new','next.','no','not','note','now','number','off','offer','office','one','outsource',
              'outsourcing','partners','person','phone','please','point','possible','president','process','product',
              'provide','reach','reaching','really','received','relevant','remove','reply','request','require',
              'respond','response','return','returning','right','sales','see','send','sender','sent','service',
              'services','set','skype','software','something','soon','sorry','spam','speak','strategy','suite',
              'support','sure','system','take','talk','team','teams','tech','thanks','think','this','though',
              'thursday','time','today','tomorrow','touch','type','unfortunately','unsubscribe','urgent','us',
              'use','want','web','week','well','will','work','working','works','yes','you']

def model_building(loc = "/home/madan/Desktop/joswin_bck/toPendrive/works/nlp-intelligence/text_modelling/"\
        "settu_sir/pipecandy_sentiment_analysis/email_text_classes.csv",rand_state=0):
    tmp = pd.read_csv(loc)
    tmp = tmp.dropna(subset=['unquoted_part_endremoved'])
    tmp = tmp[tmp['unquoted_part_endremoved']!=""]
    tmp = tmp[tmp['unquoted_part_endremoved']!= np.nan]
    tmp['Sentiment Class'] = 'Neutral'
    tmp.ix[tmp['Intent class'].isin(['Interested','Interested, schedule meeting',"need details"]),'Sentiment Class'] = 'Positive'
    tmp.ix[tmp['Intent class'].isin(["do not contact","Not interested","Not right person"]),['Sentiment Class']] = 'Negative'
    mails = tmp.ix[tmp['first_mail']==1,['unquoted_part_endremoved','Sentiment Class']]
    text_list = list(mails['unquoted_part_endremoved'])
    text_list = [i.lower() for i in text_list]
    text_list = text_vectors_list(text_list)
    vectorizer = CountVectorizer(vocabulary=vocabulary)
    x_train = vectorizer.fit_transform(text_list)
    rf = RandomForestClassifier(n_estimators=200,oob_score=True,random_state=rand_state)
    rf.fit(x_train,mails['Sentiment Class'])
    return rf,vectorizer

def model_building_short(loc = "/home/madan/Desktop/joswin_bck/toPendrive/works/nlp-intelligence/text_modelling/"\
        "settu_sir/pipecandy_sentiment_analysis/email_text_classes.csv",rand_state=0,min_sent_len=3):
    tmp = pd.read_csv(loc)
    tmp = tmp.dropna(subset=['unquoted_part_endremoved'])
    tmp = tmp[tmp['unquoted_part_endremoved']!=""]
    tmp = tmp[tmp['unquoted_part_endremoved']!= np.nan]
    tmp['Sentiment Class'] = 'Neutral'
    tmp.ix[tmp['Intent class'].isin(['Interested','Interested, schedule meeting',"need details"]),'Sentiment Class'] = 'Positive'
    tmp.ix[tmp['Intent class'].isin(["do not contact","Not interested","Not right person"]),['Sentiment Class']] = 'Negative'
    mails = tmp.ix[tmp['first_mail']==1,['unquoted_part_endremoved','Sentiment Class']]
    text_list_all = list(mails['unquoted_part_endremoved'])
    dv_list_all = list(mails['Sentiment Class'])
    text_list,dv_list = [],[]
    for i in range(len(text_list_all)):
        if len(nltk.sent_tokenize(text_list_all[i])) <= min_sent_len:
            text_list.append(text_list_all[i])
            dv_list.append(dv_list_all[i])
    print(len(dv_list))
    text_list = [i.lower() for i in text_list]
    text_list = text_vectors_list(text_list)
    vectorizer = CountVectorizer(vocabulary=vocabulary)
    x_train = vectorizer.fit_transform(text_list)
    rf = RandomForestClassifier(n_estimators=200,oob_score=True,random_state=rand_state)
    rf.fit(x_train,dv_list)
    return rf,vectorizer

