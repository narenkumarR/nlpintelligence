__author__ = 'joswin'

import re

class UrlCleaner(object):
    '''
    '''
    def __init__(self):
        '''
        :return:
        '''
        pass

    def clean_url(self,url_to_clean,secure=True):
        ''' Clean the url
        :param url:
        :return:
        '''
        #remove special characters and numbers at the beginning
        try:
            url = url_to_clean[re.search(r'[a-zA-Z]',url_to_clean).start():]
            #next check if it is starting with http. if not, add http/https based on secure flag
            if not re.match('^http',url):
                if secure:
                    url = 'https://'+url
                else:
                    url = 'http://'+url
            return url
        except:
            return url_to_clean

