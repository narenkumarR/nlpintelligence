#!/usr/bin/python
# -*- coding: utf-8 -*-

#  import xlrd
# book = xlrd.open_workbook('campaign_replies1.xlsx')
# first_sheet = book.sheet_by_index(0)
#
#
# reg_str = ''
# for tmp in prev_mail_start:
#     reg_str += '(?<='+tmp+')(.*)(?='+prev_mail_end+')|'
# reg_str = reg_str[0:-1]
# regg = re.compile()
# ## tmp = first_sheet.row_slice(rowx=1,start_colx=3,end_colx=4)[0].value

import pandas as pd,re
prev_mail_start = ['On Sun[ ,]+[\w0-9]+','On Mon','On Tue','On Wed','On Thu','On Fri','On Sat'\
                   ,'On Jan[ -/][0-9]+','On Feb[ -/][0-9]+','On Mar[ -/][0-9]+','On Apr[ -/][0-9]+','On May[ -/][0-9]+','On Jun[ -/][0-9]+'\
                    ,'On Jul[ -/][0-9]+','On Aug[ -/][0-9]+','On Sep[ -/][0-9]+','On Oct[ -/][0-9]+','On Nov[ -/][0-9]+','On Dec[ -/][0-9]+'
                   ,'On [0-9]+[ -/]Jan','On [0-9]+[ -/]Feb','On [0-9]+[ -/]Mar','On [0-9]+[ -/]Apr','On [0-9]+[ -/]May','On [0-9]+[ -/]Jun'\
                    ,'On [0-9]+[ -/]Jul','On [0-9]+[ -/]Aug','On [0-9]+[ -/]Sep','On [0-9]+[ -/]Oct','On [0-9]+[ -/]Nov','On [0-9]+[ -/]Dec'
                    ,'On [0-9]+[ -/][0-9]+[ -/][0-9]+']
prev_mail_end = 'wrote:'
reg_start = re.compile('|'.join(prev_mail_start))
reg_end = re.compile(prev_mail_end)

mails = pd.read_excel('campaign_replies1.xlsx')
ll = []
for sent in mails['unquoted_part']:
    try:
        strt = []
        for match in reg_start.finditer(sent):
            strt.append(match.start())
        strt = min(strt)
        end = []
        for match in reg_end.finditer(sent):
            end.append(match.end())
        end = max(end)
        if end>strt:
            sent1 = sent[:strt]+' '+sent[end:]
            ll.append(sent1)
        else:
            ll.append(sent)
    except:
        ll.append(sent)
ll_1 = []
for sent in ll:
    match1 = re.search(r'[\w]+ [\w]+[ ]*<[\w\.-]+@[\w\.-]+>',sent)
    if match1:
        sent = sent[:match1.start()]
    match2 = re.search(r'mailto:[\w\.-]+@[\w\.-]+',sent)
    if match2:
        sent = sent[:match2.start()]
    match3 = re.search(r'\([\w\.-]+@[\w\.-]+\)',sent)
    if match3:
        sent = sent[:match3.start()]
    match4 = re.search(r'<[\w\.-]+@[\w\.-]+',sent)
    if match4:
        sent = sent[:match4.start()]
    match5 = re.search(r'[\w]+[ ]*<[\w\.-]+@[\w\.-]+>',sent)
    if match5:
        sent = sent[:match5.start()]
    match6 = re.search(r'<[\w\.-]+@[\w\.-]+>',sent)
    if match6:
        sent = sent[:match6.start()]
    match7 = re.search(r'<[\w\.-]+@[\w\.-]+',sent)
    if match7:
        sent = sent[:match7.start()]
    ll_1.append(sent)

ll1=[]
for sent in ll_1:
    sent1 = re.sub('\\n',' ',sent)
    sent2 = re.sub(' +',' ',sent1)
    ll1.append(sent2)
mails['unquoted_part_1']=ll1

import text_preprocessing
import text_preprocessing.preprocess_text as PPP
pp = PPP.PreProcess()
tk = PPP.Tokenizer()

###########################
ll2 = []
for sent in mails['unquoted_part_1']:
    ll2.append(pp(sent))
mails['default_prepro'] = ll2

ll3 = []
for sent in mails['unquoted_part_1']:
    ll3.append(pp.without_stopword_removal(sent))
mails['without_stop'] = ll3

ll4 = []
for sent in mails['unquoted_part_1']:
    ll4.append(pp.only_lemma(sent))
mails['only_lemm'] = ll4

from nltk.corpus import stopwords
stop_words=stopwords.words('english')
remove_list = []

#################new
ll5 = []
for sent in mails['unquoted_part_1']:
    ll5.append(PPP.email_text_process(sent))
mails['unquoted_part_cleaned']=ll5

ll1 = []
for text in mails['unquoted_part_cleaned']:
    doc_tokens = tk.split_sent(text)
    doc_tokens = tk.wordnet_lemma(doc_tokens)
    doc_tokens = tk.porter_stemmer(doc_tokens)
    ll1.append(' '.join([' '.join(sent) for sent in doc_tokens]))
mails['lemma_porterstem']=ll1

ll1=[]
for text in mails['unquoted_part_cleaned']:
    doc_tokens = tk.split_sent(text)
    doc_tokens = tk.wordnet_lemma(doc_tokens)
    doc_tokens = tk.snowball_stemmer(doc_tokens)
    ll1.append(' '.join([' '.join(sent) for sent in doc_tokens]))
mails['lemma_snowballstem']=ll1


from nltk.corpus import stopwords
stop_words=stopwords.words('english')
stop_words = list(set(stop_words) - set(['not','no']))
ll1 = []
for text in mails['unquoted_part_cleaned']:
    doc_tokens = tk.split_sent(text)
    doc_tokens = tk.wordnet_lemma(doc_tokens)
    doc_tokens = tk.stopword_removal(doc_tokens,stop_words)
    doc_tokens = tk.porter_stemmer(doc_tokens)
    ll1.append(' '.join([' '.join(sent) for sent in doc_tokens]))
mails['lemma_stop_porterstem']=ll1

ll1 = []
for text in mails['unquoted_part_cleaned']:
    doc_tokens = tk.split_sent(text)
    doc_tokens = tk.wordnet_lemma(doc_tokens)
    doc_tokens = tk.stopword_removal(doc_tokens,stop_words)
    doc_tokens = tk.snowball_stemmer(doc_tokens)
    ll1.append(' '.join([' '.join(sent) for sent in doc_tokens]))
mails['lemma_stop_snowballstem']=ll1

ll1 = []
for text in mails['unquoted_part_cleaned']:
    doc_tokens = tk.split_sent(text)
    doc_tokens = tk.wordnet_lemma(doc_tokens)
    ll1.append(' '.join([' '.join(sent) for sent in doc_tokens]))
mails['only_lemma']=ll1


######################################################################################
######################################################################################
###try clustering lemmatized phrases
import pandas as pd
mails = pd.read_excel("campaign_replies1.xlsx")

from word_transformations import Tokenizer
tk = Tokenizer()
import clean_mails
ll_unqouted = []
for text in mails['unquoted_part']:
    ll_unqouted.append(clean_mails.clean_mail_text(clean_mails.fetch_first_mail_text(text)))

ll = tk.wordnet_lemma_listinput(ll_unqouted)
from nltk.corpus import stopwords
#stop_words=stopwords.words('english')
def load_stop_words(stop_word_file):
    """
    Utility function to load stop words from a file and return as a list of words
    @param stop_word_file Path and file name of a file containing stop words.
    @return list A list of stop words.
    """
    stop_words = []
    for line in open(stop_word_file):
        if line.strip()[0:1] != "#":
            for word in line.split():  # in case more than one per line
                stop_words.append(word)
    return stop_words

stop_words=load_stop_words("SmartStoplist.txt")
stop_words = list(set(stop_words) - set(['not','no','take','off','appreciate','non','look','need','believe','see']))
stop_words = list(set(stop_words+['hi','regards',u'!', u"'", u"'d", u"'ll", u"'m", u"'re", u"'s", u"'ve", u',', u'-',
    u'!', u"'", u"'d", u"'ll", u"'m", u"'re", u"'s", u"'ve", u',', u'-'
    ]))
ll1 = tk.stopword_removal_listinput(ll,stop_words)

import nltk
import chunking
import grammar as grammar_module
import extract_phrases
ch = chunking.Chunker()
gm = grammar_module.Grammar()
phr = extract_phrases.PhraseExtractor()

grammar = r"""
    NP: {<RB.*>*<DT|JJ|NN.*>+}          # Chunk sequences of DT, JJ, NN
    PP: {<IN><NP>}               # Chunk prepositions followed by NP
    VP: {<RB.*>*<VB.*><NP|PP>+} # Chunk verbs and their arguments
    CLAUSE: {<NP><VP>}           # Chunk NP, VP
    """

#using entire cleaned sentence
ll2 = ch.ne_chunk_listinput(ll)
ll3 = gm.parse_regex_grammar_listinput(ll2,grammar)

#finding phrases
#method: remove named entities, find noun phrases and verb phrases
ll4 = phr.extract_phrase_treelistinput(ll3,['NP'])
ll4_verb = phr.extract_phrase_treelistinput(ll3,['VP'])

#using sent after stopword removal
ll2_1 = ch.ne_chunk_listinput(ll1)
ll3_1 = gm.parse_regex_grammar_listinput(ll2_1,grammar)
ll4_1 = phr.extract_phrase_treelistinput(ll3_1,['NP'])
ll4_1_verb = phr.extract_phrase_treelistinput(ll3_1,['VP'])


chunk_lemma = [' $ '.join(i) for i in ll4]
chunk_lemma_stop = [' $ '.join(i) for i in ll4_1]

chunked_df = mails[[u'Email', u'Subject', u'Time']]
chunked_df['sent_lemma'] = ll
chunked_df['sent_lemma_stop'] = ll1
chunked_df['chunks_on_lemma'] = chunk_lemma
chunked_df['chunks_on_lemma_stop'] = chunk_lemma_stop

chunked_df.to_excel('chunking_results.xlsx')

#using only one grammar to identify not intereste kind of phrases
grammar = r"""
    NEG: {<RB.*><VB.*|JJ.*|>.+}
    """
ll3_neg = gm.parse_regex_grammar_listinput(ll2,grammar)
ll4_neg = phr.extract_phrase_treelistinput(ll3_neg,['NEG'])
chunked_df['phrases'] = [' $ '.join(i) for i in ll4_neg]

####################################################################
####################################################################
#clustering

#using bag of words
import pandas as pd, numpy as np
import nltk,re
from word_transformations import Tokenizer
import lda
from sklearn.feature_extraction.text import CountVectorizer,TfidfVectorizer
from nltk.stem import WordNetLemmatizer,PorterStemmer,SnowballStemmer
snowball_stemmer = SnowballStemmer('english')

def load_stop_words(stop_word_file):
    """
    Utility function to load stop words from a file and return as a list of words
    @param stop_word_file Path and file name of a file containing stop words.
    @return list A list of stop words.
    """
    stop_words = []
    for line in open(stop_word_file):
        if line.strip()[0:1] != "#":
            for word in line.split():  # in case more than one per line
                stop_words.append(word)
    return stop_words

stop_words=load_stop_words("SmartStoplist.txt")
stop_words = list(set(stop_words) - set(['not','no','take','off','appreciate','non','look','need','believe','see']))
stop_words = list(set(stop_words+['hi','regards']))

tk = Tokenizer()

mails = pd.read_excel("campaign_replies_prepro_tmp1.xls")
#LDA
text_list1 = [str(i) for i in mails['only_lemma']]
text_list = tk.stopword_removal_listinput(text_list1,stop_words)
def tokenizer(text):
    wrd_list = nltk.word_tokenize(text.lower())
    rm_list = [u'!', u"'", u"'d", u"'ll", u"'m", u"'re", u"'s", u"'ve", u',', u'-','_']
    wrd_list = [wrd for wrd in wrd_list if wrd not in rm_list]
    wrd_list = ['not' if wrd=="n't" else wrd for wrd in wrd_list]
    wrd_list = ['no' if wrd=="not" else wrd for wrd in wrd_list]
    # wrd_list = [snowball_stemmer.stem(wrd) for wrd in wrd_list]
    text = ' '.join(wrd_list)
    text = re.sub("[^a-zA-Z_ ]",' ',text)
    wrd_list = nltk.word_tokenize(text)
    wrd_list = [wrd for wrd in wrd_list if len(wrd)>1]
    wrd_list = [re.sub(r'^_+|_+$','',wrd) for wrd in wrd_list ]
    return wrd_list

vectorizer_stop = CountVectorizer(min_df=5,tokenizer=tokenizer,ngram_range=(1,2))

X_stop = vectorizer_stop.fit_transform(text_list)
vocab_dict =  {v: k for k, v in vectorizer_stop.vocabulary_.items()}
vocab = [vocab_dict[i] for i in range(max(vocab_dict.keys())+1)]
vocab = ['_'.join(wrds.split()) for wrds in vocab]

model_stop = lda.LDA(n_topics=10, n_iter=500, random_state=1)
model_stop.fit(X_stop)
topic_word_stop = model_stop.topic_word_
doc_topic_stop = model_stop.doc_topic_
n_top_words = 25
for i, topic_dist in enumerate(topic_word_stop):
    topic_words = np.array(vocab)[np.argsort(topic_dist)][:-n_top_words:-1]
    print('Topic {}: {}'.format(i, ' '.join(topic_words)))


# for i in range(200):
#     print("{} (top topic: {})".format(text_list1[i], doc_topic_stop[i].argmax()))

vectorizer_lemma = CountVectorizer(min_df=0.005,tokenizer=tokenizer)
X_lemma = vectorizer_lemma.fit_transform(text_list1)
vocab_dict =  {v: k for k, v in vectorizer_lemma.vocabulary_.items()}
vocab = [vocab_dict[i] for i in range(max(vocab_dict.keys())+1)]

model_lemma = lda.LDA(n_topics=10, n_iter=500, random_state=1)
model_lemma.fit(X_lemma)
topic_word_lemma = model_lemma.topic_word_
doc_topic_lemma = model_lemma.doc_topic_
for i, topic_dist in enumerate(topic_word_lemma):
    topic_words = np.array(vocab)[np.argsort(topic_dist)][:-n_top_words:-1]
    print('Topic {}: {}'.format(i, ' '.join(topic_words)))

lemma_cluster , stop_cluster = [],[]
for i in range(len(text_list1)):
    lemma_cluster.append(doc_topic_lemma[i].argmax())
    stop_cluster.append(doc_topic_stop[i].argmax())
tmp_df = mails[[u'Email', u'Subject','only_lemma']]
tmp_df['lemma_cluster'] = lemma_cluster
tmp_df['stop_cluster'] = stop_cluster
tmp_df.to_excel('clustering_bagofwords.xls')

#using tfidf vectorizer
from sklearn.feature_extraction.text import TfidfVectorizer
vectorizer_lemma = TfidfVectorizer(min_df=0.005,tokenizer=tokenizer)
X_lemma = vectorizer_lemma.fit_transform(text_list1)
X_lemma = np.floor(X_lemma*100)
X_lemma = X_lemma.astype(int)
#rest is same as above
vectorizer_stop = TfidfVectorizer(min_df=0.005,tokenizer=tokenizer)
X_stop = vectorizer_stop.fit_transform(text_list)
X_stop = np.floor(X_stop*100)
X_stop = X_stop.astype(int)


#using phrases
#code till getting phrases

#next step : replace phrases with single word (join phrases by _)
#ll1 is stemmed sent, ll4 & ll4_verb are phrases
import re
ll4_final = []
for i in range(len(ll4_1)):
    ll4_final.append(ll4_1[i]+ll4_1_verb[i])

def get_two_phr(phr):
    wrds = nltk.word_tokenize(phr)
    if len(wrds)<3:
        return [phr]
    else:
        new_phr = []
        for i in range(len(wrds)-1):
            new_phr.append(wrds[i]+' '+wrds[i+1])
        return new_phr

ll4_final_1 = []
for phr_list in ll4_final:
    tmpl = []
    for phr in phr_list:
        tmpl.extend(get_two_phr(phr))
        tmpl.append(phr)
    ll4_final_1.append(list(set(tmpl)))

ll5 = []
for i in range(len(ll1)):
    text = ll1[i]
    for phr in ll4_final_1[i]:
        if len(nltk.word_tokenize(phr))>1:
            new_phr = re.sub(' ','_',phr)
            text += ' '+new_phr
    ll5.append(text)
#stemmed sent not working properly (coz phrases are very less)
#try on lemma-- phrases not detected. 
import nltk



#########################################################################################
#########################################################################################
#### modelling
import pandas as pd, numpy as np
import nltk,re
from word_transformations import Tokenizer
import lda
from sklearn.feature_extraction.text import CountVectorizer,TfidfVectorizer
from nltk.stem import WordNetLemmatizer,PorterStemmer,SnowballStemmer
import sklearn
from sklearn.naive_bayes import GaussianNB,MultinomialNB
from sklearn.svm import LinearSVC,SVC
from sklearn.multiclass import OneVsRestClassifier,OneVsOneClassifier
from sklearn.ensemble import RandomForestClassifier
import extract_phrases
import chunking
from sklearn.preprocessing import MultiLabelBinarizer
import clean_mails

tk = Tokenizer()

def load_stop_words(stop_word_file):
    """
    Utility function to load stop words from a file and return as a list of words
    @param stop_word_file Path and file name of a file containing stop words.
    @return list A list of stop words.
    """
    stop_words = []
    for line in open(stop_word_file):
        if line.strip()[0:1] != "#":
            for word in line.split():  # in case more than one per line
                stop_words.append(word)
    return stop_words

stop_words=load_stop_words("SmartStoplist.txt")
from nltk.corpus import stopwords
stop_words=stop_words+stopwords.words('english')
add_list = [u'!', u"'", u"'d", u"'ll", u"'m", u"'re", u"'s", u"'ve", u',', u'-','_','www','facebook','ashwin'
    ,'youtube','linkedin','twitter','http','contractiq','techcrunch','suriyah','suriyahk','krishnan','https','hey'
    ,'iphon','hello','hi','chuck','really','probably','regards','founder','co','cofounder','de','cheers','please'
    ,'dear','best','however'
    ]
del_list = ['not','no','take','off','appreciate','non','look','need','believe','see','','what','when','where','why','how'
    ,'which','who','out','now', u'you', u'your', u'yours','in','this']
stop_words = list(set(stop_words+add_list))
stop_words = list(set(stop_words)-set(del_list))

def tokenizer(text):
    wrd_list = nltk.word_tokenize(text.lower())
    # wrd_list = [snowball_stemmer.stem(wrd) for wrd in wrd_list]
    text = ' '.join(wrd_list)
    text = re.sub("[^a-zA-Z_? ]",' ',text)
    wrd_list = nltk.word_tokenize(text)
    wrd_list = [wrd for wrd in wrd_list if len(wrd)>1 or wrd=='?']
    wrd_list = [re.sub(r'^_+|_+$','',wrd) for wrd in wrd_list ]
    return wrd_list


# stop_words = list(set(stop_words) - set(['not','no','take','off','appreciate','non','look','need','believe','see','']))
# stop_words = list(set(stop_words+['hi','regards']))
# rm_list = [u'!', u"'", u"'d", u"'ll", u"'m", u"'re", u"'s", u"'ve", u',', u'-','_','www','facebook'
#     ,'youtube','linkedin','twitter','http','contractiq','techcrunch','suriyah','suriyahk','krishnan']
# stop_words = list(set(stop_words+rm_list))


select_var = 'unquoted_part_cleaned'
dv_var = 'ActionClass'

mails = pd.read_excel("email_text_classes.xls")
dv_mat = mails[dv_var]

# dv = mails[dv_var]
# dv_list = list(dv)
# dv_list = [i.lower().split(', ') for i in dv_list]
# mlb = MultiLabelBinarizer()
# dv_mat = mlb.fit_transform(dv_list)

train_docs,test_docs,y_train,y_test = sklearn.cross_validation.train_test_split(mails[select_var],dv_mat,test_size=0.33, random_state=42)
# train_size = 1200
# data_docs,val_docs,y_data,y_val = mails.loc[:train_size-1,select_var],mails.loc[train_size:,select_var],dv_mat[:train_size],dv_mat[train_size:]
# train_docs,test_docs,y_train,y_test = sklearn.cross_validation.train_test_split(data_docs,y_data,test_size=0.30, random_state=42)

text_list1 = [str(i.encode('ascii','ignore')) if type(i)!=float else str('') for i in train_docs]

def process_textlist(text_list):
    #clenaing text
    text_list = [re.sub('\n',' ',text) for text in text_list]
    text_list = [clean_mails.clean_mail_text(sent) for sent in text_list]
    #chunking and removing named entities
    chk = chunking.Chunker()
    tree_list = chk.ne_chunk_listinput(text_list)
    names_present = [len([1 for e in list(tr) if (isinstance(e,nltk.tree.Tree) and e.label()=='PERSON')]) for tr in tree_list]
    datetime_present = [len([1 for e in list(tr) if (isinstance(e,nltk.tree.Tree) and e.label() in ['DATE','TIME'])]) for tr in tree_list]
    phrm = extract_phrases.PhraseRemover()
    text_list = phrm.remove_phrase_treelistinput(tree_list)
    #replacing no, you 
    text_list = [extract_phrases.multiple_replace({'not': 'no',"n't":'no','dont':'no','wont':'no','cant':'no','your':'you'
                                                    ,'yours':'you'},
        text.lower(),word_limit=True,flags=2) for text in text_list]
    #stopword removal and phrase removal
    text_list = tk.stopword_removal_listinput(text_list,stop_words)
    text_list = tk.stop_phrase_removal_listinput(text_list,[wrd for wrd in stop_words if len(wrd)>1])
    text_list = tk.stop_phrase_removal_listinput(text_list,["curated essays","enterprise tech","sent from"],re.IGNORECASE)
    #stemming and adding both forms
    # text_list_stem = tk.porter_stemmer_listinput(text_list)
    # tmp =[text_list[ind]+' '+text_list_stem[ind] for ind in range(len(text_list))]
    # text_list = tmp
    #merging phrases
    phm = extract_phrases.PhraseMerger()
    # phr_list = ['no need', 'no relevant'
    # , 'out office', 'take off', 'no interested'
    # , 'no see', 'email list'
    # , 'no looking', 'no something', 'mail list', 'off list'
    # , 'in house', 'no sure', u'mailing list', 'no interest', 'contact list'
    # , 'no contact'
    # , 'no work', 'take off'
    # , 'no fit', 'no look', 'no intended', 'no longer'
    # , 'no outsource','your interest'
    # ###following phrases not required
    # # ,'reach no', u'reaching out', 'service time', 'email no', 'no no', 'time no', 'interested no', u'com wrote'
    # # , u'pm com wrote', 'note no', u'currently office', u'pm com', 
    # # 'email no interested', 'no remove', 'remove mail list'
    # # , 'no thank', 'interested service'
    # ############### removed on 27 feb to check performance-- gives best performance with these
    # , 'need service', 'take look', 'feel free', 'look forward', 'interested time', u'let know'
    # , 'limited access email', 'limited access'
    # , 'no interest time', 'no interested time', 'no need service', 'no need time'
    # , 'take off list', 'stop email', 'remove mail', 'remove email', 'remove list', 'stop emailing', 'no thanks'
    # , 'please remove list', 'please remove mail', u'please remove', u'please contact' 
    # , 'request received', 'request receive'
    # , 'right now'
    # , 'good fit', u'next week', 'original message', 'reply email'
    # , 'access email', 'email address'
    # # ############## end of removed on 27 feb to check performance
    # ]
    # text_list = phm.merge_phrases_listinput(text_list,phr_list,flags=2,keep_original=True)    
    #stemming
    text_list = tk.porter_stemmer_listinput(text_list)
    # text_list = phm.merge_word_listinput(text_list,['no','you'],flags=2,with_next=True,keep_original=True)
    # phr_list_stem = list(set(["thank reach","no need","thank email","pleas remov","no thank","no interest","remov list"
    #     ,"remov mail","remov mail list","pleas remov list","pleas remov mail","request receiv"
    #     ,"feel free","take look","no interest","no thank","best regard","thank no","no interest thank"
    #     ,"let know","pleas contact","access email","limit access","great weekend","pleas excus","next week"
    #     ,"no relev","limit access email","no need servic"
    #     ]))
    phr_list_stem = [u'no need', u'out offic', u'check email', u'interest no', u'no relev', u'no thi', u'interest no contact'
    , u'thi email', u'no outsourc', u'no current', u'limit access email', u'thi no', u'need assist', u'no posit', u'stop email'
    , u'remov email', u'remov list', u'servic no', u'outsourc servic', u'no person', u'access email', u'contact no'
    , u'need contact', u'contact interest', u'reach out', u'out countri', u'mail list', u'off list', u'request receiv'
    , u'no express', u'remov mail', u'nice meet', u'email no', u'no interest', u'email address', u'repli email'
    , u'requir servic', u'thi point', u'good now', u'stop spam', u'reach out no', u'no contact', u'remov mail list'
    , u'need servic', 'thi time', u'develop hous', u'no need servic', u'great day', u'take off', u'interest remov'
    , u'take off list', u'what offer', u'limit access', u'no longer', u'now no', u'whi no', u'number call', u'out no']
    text_list = phm.merge_phrases_listinput(text_list,phr_list_stem,flags=2)
    return text_list,names_present,datetime_present

text_list, names_present,datetime_present = process_textlist(text_list1)
test_list1 = [str(i.encode('ascii','ignore')) if type(i)!=float else str('') for i in test_docs]
test_list,test_names_present,_ = process_textlist(test_list1)
# val_list1 = [str(i.encode('ascii','ignore')) if type(i)!=float else str('') for i in val_docs]
###building matrix
rand_forest_least_significant = [u'found', u'typo', u'outreach', u'answer', u'thought', u'simpli', u'trench', u'main'
    , u'instagram', u'anymor', u'contribut', u'relat', u'fax', u'internet', u'privat', u'street', u'sg', u'book'
    , u'subsidiari', u'optim', u'thx', u'disclos', u'immedi', u'control', u'distribut', u'analyt', u'media', u'polici'
    , u'prohibit', u'collabor', u'pursu', u'context', u'en', u'strateg', u'function', u'pa', u'due', u'staff', u'group'
    , u'wo', u'social', u'submit', u'sendi', u'engag', u'transmit', u'key', u'pretti', u'parikshit', u'invest', u'shortli'
    , u'repres', u'ceeeff', u'enjoy', u'notifi', u'strategi', u'andor', u'real', u'high', u'articl', u'readi', u'clear'
    , u'full', u'network', u'electron', u'paspulati', u'cto', u'sign', u'put', u'regist', u'initi', u'monitor', u'document'
    , u'novemb', u'anytim', u'coupl', u'automat', u'oliveri', u'paleocapa', u'side', u'devic', u'fabrizio', u'percassi'
    , u'membermous', u'advanc', u'commun', u'endeavour', u'strive', u'world', u'addresse', u'comment', u'tw', u'fast'
    , u'guarante', u'privileg', u'disclosur', u'dissemin', u'print', u'mistak', u'stone', u'mobileweb']
stop_words_vectorizer = list(set(stop_words+['thank','iphon','hello','hi','chuck','you','in']+rand_forest_least_significant))
vectorizer_stop = CountVectorizer(min_df=4,tokenizer=tokenizer,ngram_range=(1,1),stop_words = stop_words_vectorizer)
X_train = vectorizer_stop.fit_transform(text_list)
##using vocabulary
vocab = pd.read_csv('feature_names_bck.txt')['features']
vectorizer_stop = CountVectorizer(vocabulary=vocab,ngram_range=(1,1))
X_train = vectorizer_stop.transform(text_list)
#####

X_train = X_train.toarray()
# X_train = np.hstack([X_train,np.array(names_present).reshape((X_train.shape[0],1))])
vocab_dict =  {v: k for k, v in vectorizer_stop.vocabulary_.items()}
vocab = [vocab_dict[i] for i in range(max(vocab_dict.keys())+1)]
f = open('feature_names.txt','w')
f.write('features\n')
f.close()
with open('feature_names.txt','a') as f:
    for i in vocab:
        f.write(i+'\n')

# le = sklearn.preprocessing.LabelEncoder()
# le.fit(y_train)
# y_train1 = le.transform(y_train) 
# y_test1 = le.transform(y_test) 

gnb = MultinomialNB(fit_prior=False)
# gnb = OneVsRestClassifier(MultinomialNB(fit_prior=False))
# gnb = OneVsRestClassifier(LinearSVC(random_state=0))
# gnb = RandomForestClassifier(n_estimators=200,random_state=0)
# gnb = sklearn.linear_model.LogisticRegression(multi_class='multinomial',solver='lbfgs')
gnb.fit(X_train, y_train)
y_train_pred = gnb.predict(X_train)

X_test = vectorizer_stop.transform(test_list)
X_test = X_test.toarray()
# X_test = np.hstack([X_test,np.array(test_names_present).reshape((X_test.shape[0],1))])

y_test_pred = gnb.predict(X_test)

sklearn.metrics.f1_score(y_train,y_train_pred,average='weighted')
sklearn.metrics.f1_score(y_test,y_test_pred,average='weighted')
sklearn.metrics.accuracy_score(y_test,y_test_pred)

#################################################



##look at misclassified mails
test_mails = mails.iloc[y_test.index,:]
tmp = []
for i in range(X_test.shape[0]):
    tmp.append(','.join(np.array(vocab)[np.where(X_test[i,:]>0)[0]]))

test_mails['words_in_data'] = tmp
test_mails['NaiveBayesPrediction'] = y_test_pred
# test_mails.to_excel('Testpred_actions.xls')

#looking at train
train_mails = mails.iloc[y_train.index,:]
tmp = []
for i in range(X_train.shape[0]):
    tmp.append(','.join(np.array(vocab)[np.where(X_train[i,:]>0)[0]]))

train_mails['words_in_data'] = tmp
train_mails['NaiveBayesPrediction'] = y_train_pred
# train_mails.to_excel('Trainpred_actions.xls')

tmp = pd.concat([train_mails,test_mails], axis=0)
tmp1 = tmp.sort_index()
tmp1.to_excel('preds_all.xls')
#####################################################################
######################################################
############using textblob
from textblob.classifiers import NaiveBayesClassifier,MaxEntClassifier

#building train
y_train = list(y_train)
train_text = []
for i in range(X_train.shape[0]):
    # train_text.append(' '.join(np.array(vocab)[np.where(X_train[i,].toarray()>0)[1]]))

train_textblob = []
for ind in range(len(text_list)):
    train_textblob.append((train_text[ind],y_train[ind]))

test_text = []
for i in range(X_test.shape[0]):
    test_text.append(' '.join(np.array(vocab)[np.where(X_test[i,].toarray()>0)[1]]))

test_textblob = []
y_test = list(y_test)
for ind in range(len(test_list)):
    test_textblob.append((test_list[ind],y_test[ind]))

cl = NaiveBayesClassifier(train_textblob)
test_preds = []
for ind in range(len(test_list)):
    test_preds.append(cl.classify(test_list[ind]))
train_preds = []
for ind in range(len(text_list)):
    train_preds.append(cl.classify(text_list[ind]))
####
vocab = pd.read_csv('feature_names_bck.txt')['features']
vectorizer_stop = CountVectorizer(vocabulary=vocab,ngram_range=(1,2))
def word_extractor(document):
    tmp = vectorizer_stop.transform([document])
    tmp = tmp.toarray()
    out = {}
    for ind in range(len(vocab)):
        # out[vocab[ind]] = tmp[0,ind]
        if tmp[0,ind]>0:
            out["contains({0})".format(vocab[ind])] = True
        else:
            out["contains({0})".format(vocab[ind])] = False
    return out

cl = NaiveBayesClassifier(train_textblob,feature_extractor=word_extractor)
test_preds = []
for ind in range(len(test_list)):
    test_preds.append(cl.classify(test_list[ind]))
train_preds = []
for ind in range(len(text_list)):
    train_preds.append(cl.classify(text_list[ind]))
sklearn.metrics.f1_score(y_test,test_preds,average='weighted')
sklearn.metrics.f1_score(y_train,train_preds,average='weighted')
