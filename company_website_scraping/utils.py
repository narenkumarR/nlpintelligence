__author__ = 'joswin'

import re

def match_chars_hard(str1,str2):
    ''' Match two string
    :param str1:
    :param str2:
    :return:
    '''
    str1 = re.sub('[^a-z]','',str1.lower())
    str2 = re.sub('[^a-z]','',str2.lower())
    if re.search(str1,str2) or re.search(str2,str1):
        return True
    else:
        return False