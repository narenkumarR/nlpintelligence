from nltk.corpus import stopwords

remove_endtext_ner_mail_input = ['suriyah','krishnan','ashwin','ramaswamy']
replace_phr_input = {'not': 'no',"n't":'no','dont':'no','wont':'no','cant':'no','your':'you'
                                                    ,'yours':'you'}
stop_phrases = ["curated essays","enterprise tech"]
merge_phr_list = ['no need', 'no relevant'
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

#stop words
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

#use below if we need to use stopwords in file
# stop_words=load_stop_words("SmartStoplist.txt")

stop_words=stopwords.words('english')
stops_add_list = [u'!', u"'", u"'d", u"'ll", u"'m", u"'re", u"'s", u"'ve", u',', u'-','_','www','facebook','ashwin'
    ,'youtube','linkedin','twitter','http','contractiq','techcrunch','suriyah','suriyahk','krishnan','https','hey'
    ,'iphon','hello','hi','chuck','really','probably','regards','founder','co','cofounder','de','cheers','please'
    ]
stops_del_list = ['not','no','take','off','appreciate','non','look','need','believe','see','','what','when','where','why','how'
    ,'which','who','out','now', u'you', u'your', u'yours','in']
stop_words = list(set(stop_words+stops_add_list))
stop_words = list(set(stop_words)-set(stops_del_list))
stop_words_vectorizer = list(set(stop_words+['thank','iphon','hello','hi','chuck','you','in']))

