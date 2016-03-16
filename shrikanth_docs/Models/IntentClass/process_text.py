import nltk,re
from word_transformations import Tokenizer

import extract_phrases
from nltk.corpus import stopwords
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

# stop_words=load_stop_words("SmartStoplist.txt")
stop_words=stopwords.words('english')
add_list = [u'!', u"'", u"'d", u"'ll", u"'m", u"'re", u"'s", u"'ve", u',', u'-','_','www','facebook','ashwin'
    ,'youtube','linkedin','twitter','http','contractiq','techcrunch','suriyah','suriyahk','krishnan','https','hey'
    ,'iphon','hello','hi','chuck','really','probably','regards','founder','co','cofounder','de','cheers','please'
    ]
del_list = ['not','no','take','off','appreciate','non','look','need','believe','see','','what','when','where','why','how'
    ,'which','who','out','now', u'you', u'your', u'yours','in']
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

def process_textlist(text_list):
    '''
    :param text_list:
    :return:
    '''
    text_list = [clean_mails.fetch_first_mail_text(sent,False) for sent in text_list]
    text_list = [clean_mails.clean_mail_text(sent) for sent in text_list]
    text_list = clean_mails.remove_endtext_ner_mail_listinput(text_list,['suriyah','krishnan','ashwin','ramaswamy'])
    text_list = [extract_phrases.multiple_replace({'not': 'no',"n't":'no','dont':'no','wont':'no','cant':'no','your':'you'
                                                    ,'yours':'you'},
        text.lower(),word_limit=True,flags=2) for text in text_list]
    text_list = tk.stopword_removal_listinput(text_list,stop_words)
    text_list = tk.stop_phrase_removal_listinput(text_list,[wrd for wrd in stop_words if len(wrd)>1])
    text_list = tk.stop_phrase_removal_listinput(text_list,["curated essays","enterprise tech"])
    phm = extract_phrases.PhraseMerger()
    phr_list = ['no need', 'no relevant'
    , 'out office', 'take off', 'no interested'
    , 'no see', 'email list'
    , 'no looking', 'no something', 'mail list', 'off list'
    , 'in house', 'no sure', 'mailing list', 'no interest', 'contact list'
    , 'no contact'
    , 'no work', 'take off'
    , 'no fit', 'no look', 'no intended', 'no longer'
    , 'no outsource','your interest'
    , 'need service', 'take look', 'feel free', 'look forward', 'interested time', u'let know'
    , 'limited access email', 'limited access'
    , 'no interest time', 'no interested time', 'no need service', 'no need time'
    , 'take off list', 'stop email', 'remove mail', 'remove email', 'remove list', 'stop emailing', 'no thanks'
    , 'please remove list', 'please remove mail', u'please remove', u'please contact'
    , 'request received', 'request receive'
    , 'right now', 'good fit', u'next week', 'original message', 'reply email'
    , 'access email', 'email address'
    ]
    text_list = phm.merge_phrases_listinput(text_list,phr_list,flags=2)
    text_list = tk.porter_stemmer_listinput(text_list)
    return text_list

