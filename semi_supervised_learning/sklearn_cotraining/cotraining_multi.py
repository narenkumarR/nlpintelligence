__author__ = 'joswin'
import copy
import numpy as np
import sklearn_cotraining.classifiers
from collections import OrderedDict

class CotrainMulti(object):
    ''' support one vs all classification using co-training algorithm. Supports scikit algorithms
    '''
    def __init__(self):
        '''
        :return:
        '''
        pass

    def fit(self,clf,X_1,X_2,Y,**kwargs):
        '''
        :param clf: sklearn model object
        :param X_1: array of shape m*n
        :param X_2 : array of shape m*n
        :param Y: array of shape m*c (c is no of classes)
        :param kwargs:
        :return:
        '''
        self.model_dic = OrderedDict()
        for ind in range(Y.shape[1]):
            dv = Y[:,ind]
            model = copy.deepcopy(clf)
            clf_cotrain = sklearn_cotraining.classifiers.CoTrainingClassifier(model,**kwargs)
            clf_cotrain.fit(X_1,X_2,dv)
            self.model_dic[ind] = clf_cotrain

    def predict(self,X_1,X_2):
        '''
        :param X_1:
        :param X_2:
        :return:
        '''
        assert X_1.shape[0] == X_2.shape[0]
        preds = np.zeros((X_1.shape[0],len(self.model_dic)))
        for model_ind in self.model_dic:
            preds[:,model_ind] = self.model_dic[model_ind].predict(X_1,X_2)
        return preds

    def predict_proba(self,X_1,X_2):
        '''
        :param X_1:
        :param X_2:
        :return:
        '''
        assert X_1.shape[0] == X_2.shape[0]
        preds_neg = np.zeros((X_1.shape[0],len(self.model_dic)))
        preds_pos = np.zeros((X_1.shape[0],len(self.model_dic)))
        for model_ind in self.model_dic:
            pred = self.model_dic[model_ind].predict_proba(X_1,X_2)
            preds_neg[:,model_ind] = pred[:,0]
            preds_pos[:,model_ind] = pred[:,1]
        return preds_neg,preds_pos