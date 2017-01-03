__author__ = 'joswin'

import re
from urlparse import urljoin

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
            if not re.search('^http',url):
                if secure:
                    url = 'https://'+url
                else:
                    url = 'http://'+url
            if not re.search('www\.',url):
                if secure:
                    url = re.sub('^http(s)?://','https://www.',url)
                else:
                    url = re.sub('^http(s)?://','http://www.',url)
            return url
        except:
            return url_to_clean

    def merge_urls(self,base_url,url):
        '''
        :param url:
        :param base_url:
        :return:
        '''
        return urljoin(base_url,url)

