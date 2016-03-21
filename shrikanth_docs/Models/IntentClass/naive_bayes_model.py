import pickle
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.naive_bayes import MultinomialNB

from process_text import process_textlist,tokenizer
from constants import stop_words_vectorizer


class NaiveBayesModel(object):
    def __init__(self):
        '''
        :return:
        '''
        pass

    def fit_model(self,text_list,label_list,feature_loc='feature_names.txt'):
        '''
        :param text_list: list of texts
        :param label_list: list of labels
        :param feature_loc: location of features to be used by vectorizer
        :return:
        '''
        self.model = MultinomialNB(fit_prior=False)
        vocab = pd.read_csv(feature_loc)['features']
        if feature_loc:
            self.vectorizer = CountVectorizer(vocabulary=vocab,ngram_range=(1,1))
            X_train = self.vectorizer.transform(process_textlist(text_list))
        else:
            self.vectorizer = CountVectorizer(min_df=4,tokenizer=tokenizer,ngram_range=(1,1),\
                                              stop_words = stop_words_vectorizer)
            X_train = self.vectorizer.fit_transform(process_textlist(text_list))
        y_train = pd.Series(label_list)
        self.model.fit(X_train,y_train)

    def update_model_single(self,text,label):
        '''
        :param text:
        :param label:
        :return:
        '''
        assert label in self.model_classes
        self.model.partial_fit(self.vectorizer.transform(process_textlist([text])),pd.Series([label]))

    def update_model_listinput(self,text_list,label_list):
        '''
        :param text_list:
        :param label_list:
        :return:
        '''
        self.model.partial_fit(self.vectorizer.transform(process_textlist(text_list)),pd.Series(label_list))

    def load_model(self,model_loc='model_object.pkl',feature_loc='feature_names.txt'):
        '''
        :param model_loc:
        :param feature_loc:
        :return:
        '''
        with open(model_loc,'r') as f:
            model_dict = pickle.load(f)
        self.model_loc = model_loc
        self.model = model_dict['Model']
        self.model_classes = model_dict['Labels']
        vocab = pd.read_csv(feature_loc)['features']
        self.vectorizer = CountVectorizer(vocabulary=vocab,ngram_range=(1,1))

    def save_model(self,model_loc=None):
        '''
        :param model_loc:
        :return:
        '''
        if not model_loc:
            model_loc = self.model_loc
        with open(model_loc,'w') as f:
            pickle.dump(self.model,f)

    def save_feature_names(self,feature_loc='feature_names.txt'):
        ''' After building model using vectorizer, if we want to save the features, we can use this
        :param feature_loc:
        :return:
        '''
        vocab_dict =  {v: k for k, v in self.vectorizer.vocabulary_.items()}
        vocab = [vocab_dict[i] for i in range(max(vocab_dict.keys())+1)]
        f = open(feature_loc,'w')
        f.write('features\n')
        f.close()
        with open(feature_loc,'a') as f:
            for i in vocab:
                f.write(i+'\n')

    def predict_textinput(self,text,prob=False):
        '''
        :param text:
        :param prob:
        :return:
        '''
        if prob:
            probs = self.model.predict_proba(self.vectorizer.transform(process_textlist([text])))
            return pd.DataFrame({'Label':self.model_classes,'Probability':probs[0]})
        else:
            return self.model.predict(self.vectorizer.transform(process_textlist([text])))

    def predict_listinput(self,text_list,prob=False):
        '''
        :param text_list:
        :param prob:
        :return:
        '''
        if prob:
            print('not implemented')
            return None
        else:
            out_list = []
            for text in text_list:
                out_list.append(self.predict_textinput(text,False))
            return out_list

