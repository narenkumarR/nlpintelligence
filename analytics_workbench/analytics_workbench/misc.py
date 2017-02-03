__author__ = 'joswin'
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

class ScoreBinner(object):
    def __init__(self,score,n_bins=10):
        self.bins = pd.core.algorithms.quantile(np.unique(pd.Series(score)), np.linspace(0, 1, n_bins+1))
        self.labels = range(n_bins)

    def transform(self,score):
        return pd.tools.tile._bins_to_cuts(pd.Series(score), self.bins, include_lowest=True,labels=self.labels)