__author__ = 'joswin'

from bs4 import BeautifulSoup
import pandas as pd
import re
import urllib

import pdb

from linkedin_parser import LinkedinParserSelenium
from constants import username,password

class LinkedinSearcher(object):
    '''
    '''
    def __init__(self):
        # self.parser = LinkedinParserSelenium(username, password)
        self.max_pages = 10

    def build_search_url(self,search_dic):
        '''
        :param search_dic: dic input with parameter list.. {'title':['ceo','coo'],'company':['zendrive','hackerrank']}
        :return:
        '''
        search_string_list = []
        for key in search_dic:
            search_string_list.append(key+'='+urllib.quote(' OR '.join(search_dic[key])))
        search_string = '&'.join(search_string_list)
        if not search_string:
            raise ValueError('Not able to build url')
        url = 'https://www.linkedin.com/vsearch/p?'+search_string
        return url

    def get_all_details_from_search_soup(self,soup):
        '''
        :param soup:
        :return:
        '''
        out_list = []
        try:
            next_link_soups = soup.find('div',{'id':'results-pagination'}).findAll('li',{'class':'link'})
            if len(next_link_soups) == 1:
                self.max_pages = 1
            else:
                self.max_pages = min(10,int(next_link_soups[-2].text))
        except:
            pass

        if soup.find('div',{'id':'results-container'}).find('ol',{'id':'results','class':'search-results'}).find('li'):
            results = soup.find('div',{'id':'results-container'}).findAll('li')
            for result in results:
                try:
                    result_class = result['class']
                    if 'result' in result_class and 'people' in result_class:
                        result_dic = {}
                        result_dic['Name'] = result.find('div',{'class':'bd'}).find('h3').find('a',{'class':'title main-headline'}).getText(' ')
                        result_dic['Description'] = result.find('div',{'class':'bd'}).find('div',{'class':'description'}).getText(' ')
                        demographic_name_list = [i.text for i in result.find('div',{'class':'bd'}).find('dl',{'class':'demographic'}).findAll('dt')]
                        demographic_value_list = [i.getText(' ') for i in result.find('div',{'class':'bd'}).find('dl',{'class':'demographic'}).findAll('dd')]
                        for ind in range(min(len(demographic_name_list),min(demographic_value_list))):
                            key = demographic_name_list[ind]
                            if key in result_dic:
                                result_dic[key] = result_dic[key]+', '+demographic_value_list[ind]
                            else:
                                result_dic[key] = demographic_value_list[ind]
                        snippet_name_list = [i.text for i in result.find('dl',{'class':'snippet'}).findAll('dt')]
                        snippet_value_list = [i.getText(', ') for i in result.find('dl',{'class':'snippet'}).findAll('dd')]
                        for ind in range(min(len(snippet_name_list),len(snippet_value_list))):
                            key = snippet_name_list[ind]
                            if key == 'Current':
                                snippet_value_list[ind] = re.sub(', ',' ',snippet_value_list[ind])
                            if key in result_dic:
                                result_dic[key] = result_dic[key]+', '+snippet_value_list[ind]
                            else:
                                result_dic[key] = snippet_value_list[ind]
                        out_list.append(result_dic)
                    else:
                        continue
                except:
                    continue
        else:
            raise ValueError('The page does not have results')
        return out_list

    def get_all_details_from_searchpage(self,page,df=True):
        ''' take a linkedin search results page as input, crawl the page and return results
        :param page: page text
        :return:
        '''
        if type(page) == unicode:
            page = str(page.encode('utf-8'))
        soup = BeautifulSoup(page)
        out_list = self.get_all_details_from_search_soup(soup)
        if df:
            return pd.DataFrame(out_list)
        else:
            return out_list

    def save_excel_from_searchpage(self,page,out_loc = 'result.xls'):
        '''
        :param page:
        :return:
        '''
        res_df = self.get_details_from_searchpage(page)
        res_df.to_excel(out_loc)

    def get_all_results(self,search_dic):
        ''' fetch results from each page
        :param search_dic:
        :return:
        '''
        search_dic['titleScope'] = ['C']
        search_dic['companyScope'] = ['C']
        page_no = 1
        print('logging in')
        parser = LinkedinParserSelenium(username, password)
        out_list = []
        while page_no<=self.max_pages:
            try:
                # pdb.set_trace()
                print('page no:',page_no)
                search_dic['page_num'] = [str(page_no)]
                url = self.build_search_url(search_dic)
                text = parser.get_url(url)
                out_list.extend(self.get_all_details_from_searchpage(text,False))
            except Exception as e:
                print(e)
                break
            page_no += 1
        parser.logout()
        out_df = pd.DataFrame(out_list)
        return out_df
