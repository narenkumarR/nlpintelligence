import pickle
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.naive_bayes import MultinomialNB

from intent_class_models.text_processing.process_text import process_textlist,tokenizer
from intent_class_models.naive_bayes.constants import stop_words_vectorizer


class NaiveBayesModel(object):
    def __init__(self):
        '''
        :return:
        '''
        pass

    def fit_model(self,text_list,label_list,feature_loc='intent_class_models/naive_bayes/feature_names.txt'):
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

    def load_model(self,model_loc='intent_class_models/naive_bayes/model_object.pkl'):
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
        self.vocab = model_dict['features']
        self.vectorizer = CountVectorizer(vocabulary=self.vocab,ngram_range=(1,1))

    def save_model(self,model_loc=None):
        '''
        :param model_loc:
        :return:
        '''
        if not model_loc:
            model_loc = self.model_loc
        with open(model_loc,'w') as f:
            pickle.dump({'Model':self.model,'Labels':self.model_classes,'features':self.vocab},f)

    def save_feature_names(self,feature_loc='intent_class_models/naive_bayes/feature_names.txt'):
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

    def update_features_locinput(self,new_feat_loc = 'intent_class_models/naive_bayes/feature_names.txt'):
        '''
        :param new_feat_loc:
        :return:
        '''
        new_vocab = pd.read_csv(new_feat_loc)['features']
        self.update_features(new_vocab)

    def update_features(self,new_vocab):
        '''
        :param new_vocab:
        :return:
        '''
        l1 = list(self.vocab)
        l2 = list(new_vocab)
        log_prob1 = self.model.feature_log_prob_
        feat_count1 = self.model.feature_count_
        log_prob1,feat_count1 = self.add_features(l1,l2,log_prob1,feat_count1)
        log_prob1,feat_count1 = self.remove_features(l1,l2,log_prob1,feat_count1)
        self.model.feature_log_prob_ = log_prob1
        self.model.feature_count_ = feat_count1
        self.vocab = new_vocab
        self.vectorizer = CountVectorizer(vocabulary=self.vocab,ngram_range=(1,1))

    def add_features(self,l1,l2,log_prob1,feat_count1):
        '''
        :param l1:
        :param l2:
        :param log_prob1: numpy array
        :param feat_count1: numpy array
        :return:
        '''
        new_feats = list(set(l2)-set(l1))
        inds = [l2.index(new_feat) for new_feat in new_feats]
        inds.sort(reverse=True)
        no_classes = len(self.model_classes)
        for ind in inds:
            log_prob1 = np.concatenate([log_prob1[:,:ind],np.zeros((no_classes,1)),log_prob1[:,ind:]],axis=1)
            feat_count1 = np.concatenate([feat_count1[:,:ind],np.zeros((no_classes,1)),feat_count1[:,ind:]],axis=1)
        return log_prob1,feat_count1

    def remove_features(self,l1,l2,log_prob1,feat_count1):
        '''
        :param l1:
        :param l2:
        :param log_prob1:
        :param feat_count1:
        :return:
        '''
        rem_feats = list(set(l1)-set(l2))
        inds = [l1.index(rem_feat) for rem_feat in rem_feats]
        inds.sort(reverse=True)
        no_classes = len(self.model_classes)
        for ind in inds:
            log_prob1 = np.concatenate([log_prob1[:,:ind],log_prob1[:,ind+1:]],axis=1)
            feat_count1 = np.concatenate([feat_count1[:,:ind],feat_count1[:,ind+1:]],axis=1)
        return log_prob1,feat_count1



