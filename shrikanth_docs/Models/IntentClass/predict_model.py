import pickle
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer

from process_text import process_textlist

with open('model_object.pkl','r') as f:
    model_dict = pickle.load(f)
model = model_dict['Model']
model_classes = model_dict['Labels']

vocab = pd.read_csv('feature_names.txt')['features']
vectorizer = CountVectorizer(vocabulary=vocab,ngram_range=(1,1))

def predict_textinput(text,prob=False):
    '''
    :param text:
    :param prob:
    :return:
    '''
    if prob:
        probs = model.predict_proba(vectorizer.transform(process_textlist([text])))
        return pd.DataFrame({'Label':model_classes,'Probability':probs[0]})
    else:
        return model.predict(vectorizer.transform(process_textlist([text])))

def predict_listinput(text_list,prob=False):
    '''
    :param text_list:
    :param prob:
    :return:
    '''
    if prob:
        pass
    else:
        out_list = []
        for text in text_list:
            out_list.append(predict_textinput(text,prob))
        return out_list