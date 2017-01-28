__author__ = 'joswin'
# -*- coding: utf-8 -*-

import matplotlib.pyplot as plt
import matplotlib
matplotlib.style.use('ggplot')

from sklearn.metrics import classification_report
from sklearn.model_selection import cross_val_predict
from sklearn.metrics import roc_curve, roc_auc_score

from sklearn.model_selection import learning_curve,validation_curve
from sklearn.model_selection import ShuffleSplit

import numpy as np

def plot_learning_curve(estimator, title, X, y, ylim=None, cv=None,
                        n_jobs=1, train_sizes=np.linspace(.1, 1.0, 5)):
    """
    Generate a simple plot of the test and training learning curve.

    Parameters
    ----------
    estimator : object type that implements the "fit" and "predict" methods
        An object of that type which is cloned for each validation.

    title : string
        Title for the chart.

    X : array-like, shape (n_samples, n_features)
        Training vector, where n_samples is the number of samples and
        n_features is the number of features.

    y : array-like, shape (n_samples) or (n_samples, n_features), optional
        Target relative to X for classification or regression;
        None for unsupervised learning.

    ylim : tuple, shape (ymin, ymax), optional
        Defines minimum and maximum yvalues plotted.

    cv : int, cross-validation generator or an iterable, optional
        Determines the cross-validation splitting strategy.
        Possible inputs for cv are:
          - None, to use the default 3-fold cross-validation,
          - integer, to specify the number of folds.
          - An object to be used as a cross-validation generator.
          - An iterable yielding train/test splits.

        For integer/None inputs, if ``y`` is binary or multiclass,
        :class:`StratifiedKFold` used. If the estimator is not a classifier
        or if ``y`` is neither binary nor multiclass, :class:`KFold` is used.

        Refer :ref:`User Guide <cross_validation>` for the various
        cross-validators that can be used here.

    n_jobs : integer, optional
        Number of jobs to run in parallel (default 1).
    """
    plt.figure()
    plt.title(title)
    if ylim is not None:
        plt.ylim(*ylim)
    plt.xlabel("No of training examples")
    plt.ylabel("Score")
    train_sizes, train_scores, test_scores = learning_curve(
        estimator, X, y, cv=cv, n_jobs=n_jobs, train_sizes=train_sizes)
    train_scores_mean = np.mean(train_scores, axis=1)
    train_scores_std = np.std(train_scores, axis=1)
    test_scores_mean = np.mean(test_scores, axis=1)
    test_scores_std = np.std(test_scores, axis=1)
    plt.grid()

    plt.fill_between(train_sizes, train_scores_mean - train_scores_std,
                     train_scores_mean + train_scores_std, alpha=0.1,
                     color="r")
    plt.fill_between(train_sizes, test_scores_mean - test_scores_std,
                     test_scores_mean + test_scores_std, alpha=0.1, color="g")
    plt.plot(train_sizes, train_scores_mean, 'o-', color="r",
             label="Training score")
    plt.plot(train_sizes, test_scores_mean, 'o-', color="g",
             label="Cross-validation score")

    plt.legend(loc="best")
    return plt

class ClassificationModelDiagnotstics(object):
    ''' '''
    def __init__(self):
        pass
    
    def run_diagnostics(self,clf,X,y,binary_classification=True):
        ''' '''
        self.binary_classification = binary_classification
        # checking if the classification is multi-label classification
        if len(y.shape) ==1 or y.shape[1] == 1:
            self.multi_label = False
        else:
            self.multi_label = True
        
        # classification report
        self.cv_pred = cross_val_predict(clf,X,y,cv=10)
        print('classification report')
        print(classification_report(y,self.cv_pred))
        if self.binary_classification:
            self.cv_pred_prob = cross_val_predict(clf,X,y,cv=10,method='predict_proba')[:,1]
            print('AUC is {}'.format(roc_auc_score(y,cv_pred)))
            self.plot_auc_curve(y,self.cv_pred_prob)
            print('KS statistic')
            print(self.ks_statistic(self,cv_pred_prob,y))
        self.learning_curve_plot(clf,X,y)
        
    def plot_auc_curve(self,y,cv_pred_prob):
        fpr, tpr, _ = roc_curve(y, cv_pred_prob)
        plt.figure()
        lw = 2
        plt.plot(fpr, tpr, color='darkorange',
                 lw=lw, label='ROC curve (area = {})'.format(roc_auc_score(y,cv_pred)))
        plt.plot([0, 1], [0, 1], color='navy', lw=lw, linestyle='--')
        plt.xlim([0.0, 1.0])
        plt.ylim([0.0, 1.05])
        plt.xlabel('False Positive Rate')
        plt.ylabel('True Positive Rate')
        plt.title('Receiver operating characteristic')
        plt.legend(loc="lower right")
        plt.show()
    
    def ks_statistic(self,score,dv,n_bins=10):
        ''' assumes 1 as good and 0 as bad'''
        data = pd.DataFrame({'score':score,'dv':dv})
        data['good'] = data['dv']
        data['bad'] = 1-data['good']
        # data['bucket'] = pd.qcut(data.score, 10) #causing error
        bins = pd.core.algorithms.quantile(np.unique(data['score']), np.linspace(0, 1, n_bins+1))
        data['bucket'] = pd.tools.tile._bins_to_cuts(data['score'], bins, include_lowest=True)
        grouped = data.groupby('bucket', as_index = False)
        agg1 = pd.DataFrame({'min_scr':grouped.min().score})
        agg1['max_scr'] = grouped.max().score
        agg1['goods'] = grouped.sum().good
        agg1['bads'] = grouped.sum().bad
        agg1['total'] = agg1.bads + agg1.goods
        agg2 = (agg1.sort_index(by = 'min_scr')).reset_index(drop = True)
        agg2['odds'] = (agg2.goods / agg2.bads).apply('{0:.2f}'.format)
        agg2['bad_rate'] = (agg2.bads / agg2.total).apply('{0:.2%}'.format)
        agg2['ks'] = np.round(((agg2.bads / data.bad.sum()).cumsum() - (agg2.goods / data.good.sum()).cumsum()), 4) * 100
        flag = lambda x: '<----' if x == agg2.ks.max() else ''
        agg2['max_ks'] = agg2.ks.apply(flag)
        return agg2    
    
    def learning_curve_plot(self,estimator,X,y):
        title = "Learning Curves (Naive Bayes)"
        # Cross validation with 100 iterations to get smoother mean test and train
        # score curves, each time with 20% data randomly selected as a validation set.
        cv = ShuffleSplit(n_splits=100, test_size=0.2, random_state=0)
        plot_learning_curve(estimator, title, X, y, cv=cv, n_jobs=4,ylim=(0))
        plt.show()

    def validation_curve_plot(self,estimator,param_name,param_range,param_space='normal',
                                        scoring='f1_weighted',n_jobs=-1):
        ''' if plotting needs to be done in log space, give param_space='log', else give 'normal' '''
        train_scores, test_scores = validation_curve(
            estimator, X, y, param_name=param_name, param_range=param_range,
            cv=10, scoring=scoring, n_jobs=n_jobs)
        train_scores_mean = np.mean(train_scores, axis=1)
        train_scores_std = np.std(train_scores, axis=1)
        test_scores_mean = np.mean(test_scores, axis=1)
        test_scores_std = np.std(test_scores, axis=1)

        plt.title("Validation Curve")
        plt.xlabel("parameter values")
        plt.ylabel("Score")
        plt.ylim(0.0, 1.1)
        lw = 2
        if param_space == 'normal':
            plt.semilogx(param_range, train_scores_mean, label="Training score",
                         color="darkorange", lw=lw)
        else:
            plt.plot(param_range, train_scores_mean, label="Training score",
              color="darkorange", lw=lw)
        plt.fill_between(param_range, train_scores_mean - train_scores_std,
                         train_scores_mean + train_scores_std, alpha=0.2,
                         color="darkorange", lw=lw)
        plt.semilogx(param_range, test_scores_mean, label="Cross-validation score",
                     color="navy", lw=lw)
        plt.fill_between(param_range, test_scores_mean - test_scores_std,
                         test_scores_mean + test_scores_std, alpha=0.2,
                         color="navy", lw=lw)
        plt.show()
    
