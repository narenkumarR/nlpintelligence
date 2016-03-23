__author__ = 'joswin'

from bs4 import BeautifulSoup

from linkedin_parser import LinkedinParserSelenium
from constants import username,password

class LinkedinSearcher(object):
    '''
    '''
    def __init__(self):
        self.parser = LinkedinParserSelenium(username, password)

    def get_search_soup_dicinput(self,search_dic):
        '''
        :param search_dic: dic input with parameter list.. {'Title':['ceo','coo'],'Company':['zendrive','hackerrank']}
        :return:
        '''
        search_string_list = []
        for key in search_dic:
            search_string_list.append(' OR '.join(search_dic[key]))
        search_string = '&'.join(search_string_list)
        if not search_string:
            return ''
        url = 'https://www.linkedin.com/vsearch/p?'+search_string
        soup = self.parser.get_soup(url)
        return soup



