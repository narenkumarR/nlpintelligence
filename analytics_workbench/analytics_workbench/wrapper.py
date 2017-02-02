__author__ = 'joswin'
# -*- coding: utf-8 -*-

from .preprocessing import PreProcessor
from .supervised_learning import ClassificationModelCV
from .model_diagnostics import ClassificationModelDiagnotstics

class Wrapper(object):
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
