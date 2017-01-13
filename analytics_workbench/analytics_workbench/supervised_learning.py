__author__ = 'joswin'
# -*- coding: utf-8 -*-

from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import LogisticRegression
from sklearn.svm import LinearSVC,SVC
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV

default_grid_search_dic = {
    'multinomial naive bayes':{
        'alpha':[0,0.01,0.1,0.5,1,10],
        'fit_prior':[True,False]
    },
    'logistic regression' : {
        'penalty':['l1','l2'],
        'C':[0.001,0.01,0.1,1,10,50,100],
        'class_weight':['balanced',None]
    },
    'linear svm': {
        'penalty':['l1','l2'],
        'C':[0.001,0.01,0.1,1,10,50,100],
        'class_weight':['balanced',None],
        'loss':['hinge','squared_hinge']
    },
    'svm' : {
        'C':[0.001,0.01,0.1,1,10,50,100],
        'kernel': ['linear', 'poly', 'rbf', 'sigmoid'],
        'class_weight':['balanced',None]
    },
    'random forest': {
        'n_estimators':[10,50,200,500],
        'max_depth' : [None,5,10,20],
        'min_samples_leaf' : [1,5,20,50],
        'min_weight_fraction_leaf' : [0,0.1,0.3]
    }
}

class ClassificationModelCV(object):
    '''
    wrappers for models
    '''
    def __init__(self,model_algorithm='',grid_search_dic=None,model_obj=None,scoring='f1',**kwargs):
        '''
        :param model_algorithm: string name
        :param grid_search_dic: grid search dictionary
        :param model_obj: give the model object directly instead of giving string in model_algorithm
        :param scoring: the measure for selecting the best estimator, default f1 score.passed to GridSearchCV
        :param kwargs: give additional arguments to the model algorithm here.
        :return:
        '''
        if model_obj:
            if not grid_search_dic:
                raise ValueError('give grid search dictionary')
            clf = model_obj
        elif model_algorithm == 'multinomial naive bayes':
            clf = MultinomialNB(**kwargs) #fit_prior and class_prior
        elif model_algorithm == 'logistic regression':
            clf = LogisticRegression(**kwargs)
        elif model_algorithm == 'linear svm':
            clf = LinearSVC(**kwargs)
        elif model_algorithm == 'svm':
            clf = SVC(**kwargs)
        elif model_algorithm == 'random forest':
            clf = RandomForestClassifier(**kwargs)
        else:
            raise ValueError('Model not implemented now. Please try to implement')
        if not grid_search_dic:
            grid_search_dic = default_grid_search_dic[model_algorithm]
        self.clf_search = GridSearchCV(clf, grid_search_dic,scoring=scoring)

    def fit(self,X,y,**kwargs):
        '''
        :param X:
        :param y:
        :param kwargs:
        :return:
        '''
        self.clf_search.fit(X,y,**kwargs)

    def get_cv_result(self):
        ''' get the performance of options in the cross-validation. This can be used for optimizing the parameters
        :return:
        '''
        return self.clf_search.cv_results_

    def get_best_model(self):
        '''
        :return:
        '''
        return self.clf_search.best_estimator_

    def predict(self,X,**kwargs):
        '''
        :param X:
        :param kwargs:
        :return:
        '''
        return self.clf_search.predict(X,**kwargs)

    def predict_proba(self,X,**kwargs):
        '''
        :param X:
        :param kwargs:
        :return:
        '''
        return self.clf_search.predict_proba(X,**kwargs)