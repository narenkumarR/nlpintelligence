#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'joswin'
import cPickle as pickle
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.naive_bayes import MultinomialNB,BernoulliNB

from process_text import process_textlist,tokenizer
# from naive_bayes.constants import stop_words_vectorizer

class NaiveBayesModel(object):
    def __init__(self):
        '''
        :return:
        '''
        # self.default_class_hardcode = np.array([u'random'],dtype='<U28')
        pass

    def _process_textlist(self,text_list):
        '''
        :param text_list:
        :return:
        '''
        return process_textlist(text_list,stem=self.stem,replace_phr_input=self.replace_phr_input,
                                         stop_words=self.stop_words,stop_phrases=self.stop_phrases,
                                         merge_phr_list_keep_original=self.merge_phr_list_keep_original,
                                         merge_phr_list_remove_original=self.merge_phr_list_remove_original,
                                         merge_words_with_next_keep_original=self.merge_words_with_next_keep_original,
                                         merge_words_with_next_remove_original=self.merge_words_with_next_remove_original)

    def _gen_samples_uniform(self,text_list,label_list=[]):
        '''
        :param text_list:
        :param label_list:
        :return:
        '''
        if not label_list:
                raise ValueError('Need to give label list for sampling')
        # each class will be sampled to len(text_list)
        df = pd.DataFrame({'text':text_list,'dv':label_list})
        if self.class_wts:
            tmp = df.groupby('dv').apply(
                lambda x: x.sample(int(df.shape[0]*self.class_wts[self.classes.index(list(x['dv'])[0])]*3),
                                   replace=True,random_state=self.random_state))
        else:
            tmp = df.groupby('dv').apply(lambda x: x.sample(df.shape[0],replace=True))
        return list(tmp['text']),list(tmp['dv'])

    def _gen_vectorizer_get_dtm_textlist(self,text_list,label_list=[],feature_loc='',min_df=4):
        '''create vectorizer and generate dtm
        :param text_list:
        :return:
        '''
        if feature_loc:
            if type(feature_loc) == str:
                self.vocab = pd.read_csv(feature_loc)['features']
            else:
                self.vocab = feature_loc
            self.vectorizer = CountVectorizer(vocabulary=self.vocab,ngram_range=self.n_gram_range)
            # need to do uniform sampling here because otherwise some probs can occur
            # naive bayes gives bad results when the number is
            # text_list,label_list = self._gen_samples_uniform(text_list,label_list)
            X_train = self.vectorizer.transform(self._process_textlist(text_list))
        else:
            self.vectorizer = CountVectorizer(min_df=min_df,tokenizer=tokenizer,ngram_range=self.n_gram_range,\
                                              stop_words = self.stop_words_vectorizer)
            X_train = self.vectorizer.fit_transform(self._process_textlist(text_list))
            self.vocab = self.vectorizer.get_feature_names()
        return X_train,label_list

    def _get_dtm_text(self,text):
        '''
        :param text:
        :return:
        '''
        return self.vectorizer.transform(self._process_textlist([text]))

    def _get_dtm_textlist(self,textlist):
        '''
        :param textlist:
        :return:
        '''
        return self.vectorizer.transform(self._process_textlist(textlist))

    def fit_model(self,text_list,label_list,stem,
                  replace_phr_input,stop_words,stop_phrases,
                  merge_phr_list_keep_original,
                  merge_phr_list_remove_original,merge_words_with_next_keep_original,
                  merge_words_with_next_remove_original,
                  stop_words_vectorizer=None,
                  n_gram_range = (1,1),
                  feature_loc=None,
                  default_class='',min_df=4,model_method = 'MultinomialNB',random_state=0,
                  **kwargs):
        '''
        :param text_list:
        :param label_list:
        :param stem:
        :param replace_phr_input:
        :param stop_words:
        :param stop_phrases:
        :param merge_phr_list_keep_original:
        :param merge_phr_list_remove_original:
        :param merge_words_with_next_keep_original:
        :param merge_words_with_next_remove_original:
        :param stop_words_vectorizer :
        :param n_gram_range : if simply take all ngrams, give this value and set all different lists as [](not implemented)
        :param feature_loc: either a list of features or location to csv file (quick fix, need to do properly)
        :param default_class:
        :param min_df:
        :param model_method:
        :param kwargs:
        :return:
        '''
        self.random_state = random_state
        self.stem = stem
        self.replace_phr_input = replace_phr_input
        self.stop_words = stop_words
        self.stop_phrases = stop_phrases
        self.merge_phr_list_keep_original = merge_phr_list_keep_original
        self.merge_phr_list_remove_original = merge_phr_list_remove_original
        self.merge_words_with_next_keep_original = merge_words_with_next_keep_original
        self.merge_words_with_next_remove_original = merge_words_with_next_remove_original
        self.n_gram_range = n_gram_range

        if not stop_words_vectorizer:
            self.stop_words_vectorizer = stop_words
        else:
            self.stop_words_vectorizer = stop_words_vectorizer
        # Default class. This class will have more prior probability compared to other classes
        self.classes = list(np.unique(label_list))
        self.classes.sort()
        if default_class:
            if 'class_prior' in kwargs:
                del kwargs['class_prior']
            assert default_class in self.classes
            self.default_class = np.array([default_class],dtype='|S8')
            default_class_ind = self.classes.index(default_class)
            class_wts = [0.95/len(self.classes)]*(len(self.classes)-1)
            default_class_wt = 0.95/len(self.classes)+0.05
            self.class_wts = class_wts[:default_class_ind]+[default_class_wt]+class_wts[default_class_ind:]
        else:
            self.class_wts = None
            self.default_class = None
        if model_method == 'MultinomialNB':
            self.model = MultinomialNB(fit_prior=False,class_prior=self.class_wts,**kwargs)
        elif model_method == 'BernoulliNB':
            self.model = BernoulliNB(fit_prior=False,class_prior=self.class_wts,**kwargs)
        elif model_method == 'GaussianNB':
            # self.model = GaussianNB()
            raise ValueError('This method does not support sparse array as input, so not implemented now. Can add later')
        else:
            raise ValueError('Check the model_method input')
        X_train,label_list = self._gen_vectorizer_get_dtm_textlist(text_list,label_list,feature_loc=feature_loc,min_df=min_df)
        y_train = pd.Series(label_list)
        self.model.fit(X_train,y_train)
        self.model_classes = list(self.model.classes_)
        self.class_prior_weights = self.class_wts

    def update_model_single(self,text,label):
        '''update the model with text and label
        :param text:
        :param label:
        :return:
        '''
        assert label in self.model_classes
        self.model.partial_fit(self._get_dtm_text(text),pd.Series([label]))

    def update_model_listinput(self,text_list,label_list):
        '''update model for multiple texts
        :param text_list: input text list
        :param label_list: text classes for each text
        :return:
        '''
        for ll in label_list:
            assert ll in self.model_classes
        self.model.partial_fit(self._get_dtm_textlist(text_list),pd.Series(label_list))

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

    def load_model(self,model_loc='intent_class_models/naive_bayes/model_object.pkl'):
        '''Work in progress. Don't use
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
        '''Work in progress. Don't use
        :param model_loc:
        :return:
        '''
        if not model_loc:
            model_loc = self.model_loc
        with open(model_loc,'w') as f:
            pickle.dump({'Model':self.model,'Labels':self.model_classes,'features':self.vocab},f)

    def predict_textinput(self,text,prob=False):
        '''
        :param text:
        :param prob:
        :return:
        '''
        if prob:
            probs = self.model.predict_proba(self._get_dtm_text(text))
            return pd.DataFrame({'Label':self.model_classes,'Probability':probs[0]})
        else:
            vec_rep = self._get_dtm_text(text)
            if vec_rep.sum() == 0:
                return self.default_class
            else:
                return self.model.predict(vec_rep)

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
            vec_rep = self._get_dtm_textlist(text_list)
            return self.model.predict(vec_rep)

    def replace_features_locinput(self,new_feat_loc = 'intent_class_models/naive_bayes/feature_names.txt'):
        '''Replace the model with new features (words,phrases). the location should contain all the features for the
        new model. Generally, feature_names.txt will have the features present in the current model. If we want to add
        a new feature or remove an existing feature, update it in the feature_names.txt file, and call this function.
        :param new_feat_loc: a csv/txt file, there should be a column with name "features"
        :return:
        '''
        new_vocab = pd.read_csv(new_feat_loc)['features']
        self.replace_features(new_vocab)

    def replace_features(self,new_vocab):
        '''Replacing the features of the model with features in new_vocab. A key assumption is that the words are just
            added or removed in the actual words list, but the order remains same.
            Eg: original_list = ['hi','interest','not']
                new_vocab = ['hi','not','what'] # here interest is removed and what is added, but the order of other
                                                words have not changed
        :param new_vocab: list of features
        :return:
        '''
        l1 = list(self.vocab)
        l2 = list(new_vocab)
        log_prob1 = self.model.feature_log_prob_
        feat_count1 = self.model.feature_count_
        #Find new features and add them to the model
        log_prob1,feat_count1 = self._replace_features_add(l1,l2,log_prob1,feat_count1)
        #Find the features not present in the new feature list and remove them
        log_prob1,feat_count1 = self._replace_features_remove(l1,l2,log_prob1,feat_count1)
        self.model.feature_log_prob_ = log_prob1
        self.model.feature_count_ = feat_count1
        self.vocab = new_vocab
        self.vectorizer = CountVectorizer(vocabulary=self.vocab)

    def _replace_features_add(self,l1,l2,log_prob1,feat_count1):
        '''If a feature is present in l1 and l2, nothing is done. If a feature is present in l2, but not l1, they
            are added to the model. Not intended to call directly
        :param l1:list of features in the current model
        :param l2: features in the new model
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
            # Trying to give log of prior probability. But the output probabilities are different. with 0 gives correct results
            # log_prob1 = np.concatenate([log_prob1[:,:ind],np.reshape(np.log(self.class_prior_weights),(len(self.model_classes),1)),log_prob1[:,ind:]],axis=1)
            feat_count1 = np.concatenate([feat_count1[:,:ind],np.zeros((no_classes,1)),feat_count1[:,ind:]],axis=1)
        return log_prob1,feat_count1

    def _replace_features_remove(self,l1,l2,log_prob1,feat_count1):
        '''If a feature is present in l1 and l2, nothing is done. If a feature is present in l1, but not in l2, they are
            removed. Not intended to call directly
        :param l1: list of features in the current model
        :param l2: features in the new model
        :param log_prob1:
        :param feat_count1:
        :return:
        '''
        rem_feats = list(set(l1)-set(l2))
        inds = [l1.index(rem_feat) for rem_feat in rem_feats]
        inds.sort(reverse=True)
        # no_classes = len(self.model_classes)
        for ind in inds:
            log_prob1 = np.concatenate([log_prob1[:,:ind],log_prob1[:,ind+1:]],axis=1)
            feat_count1 = np.concatenate([feat_count1[:,:ind],feat_count1[:,ind+1:]],axis=1)
        return log_prob1,feat_count1

    def update_features(self,feat_to_add=[],feat_to_remove=[]):
        ''' Add new features to the model(this is called when a list of features to add and a list of features
                to remove are with us. To completely replace with a new feature list, use the replace_features function)
        :param feat_to_add: list of features to be added to the model
        :param feat_to_remove: list of features to be removed from the model
        :return:
        '''
        if not feat_to_add and not feat_to_remove:
            raise ValueError('Need to provide features to add or features to remove')
        feat_to_add = [i for i in feat_to_add if i not in self.vocab]
        l1 = list(self.vocab)
        l2 = list(self.vocab)
        if feat_to_add:
            l2 = l1+list(feat_to_add)
        if feat_to_remove:
            l2 = [i for i in l2 if i not in feat_to_remove]
        log_prob1 = self.model.feature_log_prob_
        feat_count1 = self.model.feature_count_
        #Find new features and add them to the model
        log_prob1,feat_count1 = self._replace_features_add(l1,l2,log_prob1,feat_count1)
        #Find the features not present in the new feature list and remove them
        log_prob1,feat_count1 = self._replace_features_remove(l1,l2,log_prob1,feat_count1)
        self.model.feature_log_prob_ = log_prob1
        self.model.feature_count_ = feat_count1
        self.vocab = l2
        self.vectorizer = CountVectorizer(vocabulary=self.vocab,ngram_range=(1,1))



