__author__ = 'joswin'
import re
from nltk import word_tokenize

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
    text = ' '.join([wrd for wrd in word_tokenize(text) if wrd not in stopwrds_full])
    text = re.sub(r"'",' ',text)
    text = re.sub(r' +',' ',text)
    text = ' '.join(wrd for wrd in text.split(' ') if len(wrd)>1)
    return text

def text_vectors_list(text_list):
    return [text_vectors(text) for text in text_list]
