"""
File : string_distance.py
Created On: 07-Mar-2016
Author: ideas2it
"""

import Levenshtein

def distance_btn_strings(str1,str2):
    '''
    :param str1:
    :param str2:
    :return:
    '''
    return Levenshtein.distance(str1,str2)

