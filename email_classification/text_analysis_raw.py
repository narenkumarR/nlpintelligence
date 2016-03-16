import pandas as pd
mails = pd.read_excel("campaign_replies1.xlsx")

from word_transformations import Tokenizer
tk = Tokenizer()

#cleaning text
import clean_mails
ll_unqouted = []
for text in mails['unquoted_part']:
    ll_unqouted.append(clean_mails.clean_mail_text(clean_mails.fetch_first_mail_text(text)))

#lemmatization
ll = tk.wordnet_lemma_listinput(ll_unqouted)

#remove stopwords
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
stop_words = list(set(stop_words+['hi','regards']))
ll1 = tk.stopword_removal_listinput(ll,stop_words)

#extracting phrases
import nltk
import chunking
import grammar
import extract_phrases
ch = chunking.Chunker()
gm = grammar.Grammar()
phr = extract_phrases.PhraseExtractor()

#for lemmatized text
#chunking
ll2 = ch.chunk_sent_listinput(ll)
#regex parser
grammar = r"""
    NEG: {<RB.*><VB.*|JJ.*|>.+}
    """
ll3_neg = gm.parse_regex_grammar_listinput(ll2,grammar)
ll4_neg = phr.extract_phrase_treelistinput(ll3_neg,'NEG')
chunked_df = mails[[u'Email', u'Subject', u'Time']]
chunked_df['sent_lemma'] = ll
chunked_df['phrases'] = [' $ '.join(i) for i in ll4_neg]

#for text where stopwords are removed
ll2_1 = ch.chunk_sent_listinput(ll1)
ll3_1 = gm.parse_regex_grammar_listinput(ll2_1,grammar)
ll4_1 = phr.extract_phrase_treelistinput(ll3_1,'NEG')
chunked_df['phrases_stopremove'] = chunk_lemma_stop

chunked_df.to_excel('phrase_extraction_only_negative_response.xls')

