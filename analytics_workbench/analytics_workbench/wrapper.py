__author__ = 'joswin'
# -*- coding: utf-8 -*-

class SupervisedWrapper(object):
    '''
    '''
    def __init__(self,pre_processor,model_obj):
        '''
        :param pre_processor:
        :param model_obj:
        :return:
        '''
        self.pre_processor = pre_processor
        self.model_obj = model_obj

    def predict(self,inp_df,**kwargs):
        '''
        :param inp_df:
        :return:
        '''
        return self.model_obj.predict(self.pre_processor.get_matrix_test(inp_df),**kwargs)

class UnSupervisedWrapper(object):
    '''
    '''
    def __init__(self,pre_processor,model_obj):
        '''
        :param pre_processor:
        :param model_obj:
        :return:
        '''
        self.pre_processor = pre_processor
        self.model_obj = model_obj

    def transform(self,inp_df,**kwargs):
        '''
        :param inp_df:
        :return:
        '''
        return self.model_obj.transform(self.pre_processor.get_matrix_test(inp_df),**kwargs)