from sklearn.naive_bayes import GaussianNB,MultinomialNB
import nltk

def default_tokenizer(text):
    wrd_list = nltk.word_tokenize(text.lower())
    # wrd_list = [snowball_stemmer.stem(wrd) for wrd in wrd_list]
    text = ' '.join(wrd_list)
    text = re.sub("[^a-zA-Z_? ]",' ',text)
    wrd_list = nltk.word_tokenize(text)
    wrd_list = [wrd for wrd in wrd_list if len(wrd)>1 or wrd=='?']
    wrd_list = [re.sub(r'^_+|_+$','',wrd) for wrd in wrd_list ]
    return wrd_list

class BuildFeatureMatrix(object):
	"""docstring for BuildFeatureMatrix"""
	def __init__(self):
		pass

	def build_sklearn_dtm_listinput(self,text_list,tokenizer=None,feature_list=None):
		''' '''
		if not tokenizer:
			tokenizer = default_tokenizer
		

		


class TextModel(object):
	def __init__(self,model_fun):
		pass