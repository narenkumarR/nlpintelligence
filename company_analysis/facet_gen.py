# -*- coding: utf-8 -*-
__author__ = 'joswin'

import numpy as np,scipy
from scipy.stats import rankdata

class FacetWordsGen():
    '''
    '''
    def __init__(self):
        pass

    def get_freq_shift(self,mat_context,mat_orig):
        '''freq_context - freq_orig (freq : count wrd/total wrds)
        :param mat_context:
        :param wrds_context:
        :param mat_orig:
        #:param wrds_orig:should be same as wrds_context
        :return: matrix : wrds,freq_shift_value
        '''
        # all_wrds = list(set(wrds_context+wrds_orig))
        context_tot_cnt = float(scipy.sparse.csr_matrix.sum(mat_context))
        orig_tot_cnt = float(scipy.sparse.csr_matrix.sum(mat_orig))
        context_cnts = scipy.sparse.csr_matrix.sum(mat_context,0)
        orig_cnts = scipy.sparse.csr_matrix.sum(mat_orig,0)
        freq_shift_val = context_cnts/context_tot_cnt - orig_cnts/orig_tot_cnt
        # freq_shift_val = context_cnts - orig_cnts
        return freq_shift_val.transpose()

    def get_rank_shift(self,mat_context,mat_orig):
        ''' log2(rank_orig) - log2(rank_context) (rank :order each word by frequency )
        :param mat_context:
        :param wrds_context:
        :param mat_orig:
        :param wrds_orig:
        :return:matrix : wrds,rank_shift_value
        '''
        context_cnts = scipy.sparse.csr_matrix.sum(mat_context,0)
        orig_cnts = scipy.sparse.csr_matrix.sum(mat_orig,0)
        context_ranks = (1+context_cnts.shape[1]) - rankdata(context_cnts,method='min')
        orig_ranks = (1+orig_cnts.shape[1]) - rankdata(orig_cnts,method='min')
        context_ranks_log2 = np.log(context_ranks)/np.log(2)
        orig_ranks_log2 = np.log(orig_ranks)/np.log(2)
        return orig_ranks_log2-context_ranks_log2

    def log_likelihood(self,p,k,n):
        '''
        :param p:
        :param k:
        :param n:
        :return:
        '''
        return k*np.log(p+0.000000000000001) + (n-k)*np.log(1-(p+0.000000000000001))

    def get_log_likelihood_statistic(self,mat_context,mat_orig):
        '''
         for word t, dfC is word count in context data, df is word count in original data
         and D is the total number of words in the original data
         − log λt = log L(p1 , dfC , |D|) + log L(p2 , df , |D|)− log L(p, df , |D|) − log L(p, dfC , |D|)
         log L(p, k, n) = k log(p) + (n − k) log(1 − p),
         p1 = dfC/|D| , p2 = df/|D| , and p = (p1 +p2 .)/2
        :param mat_context:
        :param mat_orig:
        :return:
        '''
        d_count = float(scipy.sparse.csr_matrix.sum(mat_orig))
        context_cnts = scipy.sparse.csr_matrix.sum(mat_context,0)
        orig_cnts = scipy.sparse.csr_matrix.sum(mat_orig,0)
        p1 = context_cnts/d_count
        p2 = orig_cnts/d_count
        p = (p1+p2)/2
        log_likelihood_fun = self.log_likelihood(np.asarray(p1),np.asarray(context_cnts),d_count) + \
                             self.log_likelihood(np.asarray(p2),np.asarray(orig_cnts),d_count) - \
                             self.log_likelihood(np.asarray(p),np.asarray(context_cnts),d_count) - \
                             self.log_likelihood(np.asarray(p),np.asarray(orig_cnts),d_count)
        return 2*log_likelihood_fun.transpose()

    def get_p_value(self,log_likelihoods,df=1):
        '''
        :param log_likelihoods:
        :return:
        '''
        return 1 - scipy.stats.chi2.cdf(log_likelihoods, df)

class Subsumption(object):
    '''
    '''
    def __init__(self):
        pass

    def get_conditional_probs(self,mat):
        ''' p(a/b) = p(aUb)/p(b)
        :param mat: brinary matrix
        :return:
        '''
        a_u_b = mat.transpose().dot(mat) #this will be row based.ie, row 1 will have nos related to wrd1
        wrd_cnts = scipy.sparse.csr_matrix.sum(mat,0)
        cond_probs = a_u_b.transpose() / wrd_cnts #division is done col based.. this is col based
        return cond_probs

    def condition_1(self,cond_probs):
        ''' a/b>0.8 and b/a<1
        :param mat: output of get_conditional_probs function (cond prob of wrd/wrd_i is given in ith column
        :return:
        '''
        cond_probs = np.array(cond_probs)
        cond_probs_t = cond_probs.transpose()
        cond1 = cond_probs >= 0.8
        cond2 = cond_probs_t <1
        match_inds = np.where((cond1*cond2)==True)
