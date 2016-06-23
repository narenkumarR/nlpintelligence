import pickle
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.naive_bayes import MultinomialNB,BernoulliNB,GaussianNB

from intent_class_models.text_processing.process_text import process_textlist,tokenizer
from intent_class_models.naive_bayes.constants import stop_words_vectorizer

class NaiveBayesModel(object):
    def __init__(self):
        '''
        :return:
        '''
        self.default_class_hardcode = np.array([u'random'],dtype='<U28')

    def fit_model(self,text_list,label_list,feature_loc='intent_class_models/naive_bayes/feature_names.txt',
                  default_class='',model_method = 'MultinomialNB',**kwargs):
        '''
        :param text_list: list of texts
        :param label_list: list of labels
        :param feature_loc: location of features to be used by vectorizer
        :param kwargs : input to model
        :return:
        '''
        # Default class. This class will have more prior probability compared to other classes
        if default_class:
            if 'class_prior' in kwargs:
                del kwargs['class_prior']
            classes = list(np.unique(label_list))
            classes.sort()
            assert default_class in classes
            default_class_ind = classes.index(default_class)
            class_wts = [0.95/len(classes)]*(len(classes)-1)
            default_class_wt = 0.95/len(classes)+0.05
            class_wts = class_wts[:default_class_ind]+[default_class_wt]+class_wts[default_class_ind:]
        else:
            class_wts = None
        if model_method == 'MultinomialNB':
            self.model = MultinomialNB(fit_prior=False,class_prior=class_wts,**kwargs)
        elif model_method == 'BernoulliNB':
            self.model = BernoulliNB(fit_prior=False,class_prior=class_wts,**kwargs)
        elif model_method == 'GaussianNB':
            # self.model = GaussianNB()
            raise ValueError('This method does not support sparse array as input, so not implemented now. Can add later')
        else:
            raise ValueError('Check the model_method input')
        if feature_loc:
            self.vocab = pd.read_csv(feature_loc)['features']
            self.vectorizer = CountVectorizer(vocabulary=self.vocab,ngram_range=(1,1))
            X_train = self.vectorizer.transform(process_textlist(text_list))
        else:
            self.vectorizer = CountVectorizer(min_df=4,tokenizer=tokenizer,ngram_range=(1,1),\
                                              stop_words = stop_words_vectorizer)
            X_train = self.vectorizer.fit_transform(process_textlist(text_list))
            self.vocab = self.vectorizer.get_feature_names()
        y_train = pd.Series(label_list)
        self.model.fit(X_train,y_train)
        self.model_classes = list(self.model.classes_)
        self.class_prior_weights = class_wts


    def update_model_single(self,text,label):
        '''update the model with text and label
        :param text:
        :param label:
        :return:
        '''
        assert label in self.model_classes
        self.model.partial_fit(self.vectorizer.transform(process_textlist([text])),pd.Series([label]))

    def update_model_listinput(self,text_list,label_list):
        '''update model for multiple texts
        :param text_list: input text list
        :param label_list: text classes for each text
        :return:
        '''
        for ll in label_list:
            assert ll in self.model_classes
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
            vec_rep = self.vectorizer.transform(process_textlist([text]))
            if vec_rep.sum() == 0:
                return self.default_class_hardcode
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
            out_list = []
            for text in text_list:
                out_list.append(self.predict_textinput(text,False))
            return out_list

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
        log_prob1,feat_count1 = self.replace_features_add(l1,l2,log_prob1,feat_count1)
        #Find the features not present in the new feature list and remove them
        log_prob1,feat_count1 = self.replace_features_remove(l1,l2,log_prob1,feat_count1)
        self.model.feature_log_prob_ = log_prob1
        self.model.feature_count_ = feat_count1
        self.vocab = new_vocab
        self.vectorizer = CountVectorizer(vocabulary=self.vocab,ngram_range=(1,1))

    def replace_features_add(self,l1,l2,log_prob1,feat_count1):
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

    def replace_features_remove(self,l1,l2,log_prob1,feat_count1):
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
        ''' Add new features to the model
        :param feat_to_add:
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
        log_prob1,feat_count1 = self.replace_features_add(l1,l2,log_prob1,feat_count1)
        #Find the features not present in the new feature list and remove them
        log_prob1,feat_count1 = self.replace_features_remove(l1,l2,log_prob1,feat_count1)
        self.model.feature_log_prob_ = log_prob1
        self.model.feature_count_ = feat_count1
        self.vocab = l2
        self.vectorizer = CountVectorizer(vocabulary=self.vocab,ngram_range=(1,1))



