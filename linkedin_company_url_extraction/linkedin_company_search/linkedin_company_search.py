__author__ = 'joswin'

import re
import urllib
from bs4 import BeautifulSoup

from selenium_crawl import SeleniumParser
from linkedin_credentials import username,password

class LinkedinParser(SeleniumParser):
    def __init__(self,browser = 'Firefox',browser_loc='/home/madan/Downloads/phantomjs-2.1.1-linux-x86_64/bin/phantomjs',
                 visible = True,proxy = False,proxy_ip=None,proxy_port=None,page_load_timeout=100,use_tor=False):
        SeleniumParser.__init__(self,browser,browser_loc,visible,proxy,proxy_ip,proxy_port,page_load_timeout,use_tor)
        self.login(username,password)

    def login(self,username,password):
        '''
        :return:
        '''
        # self.browser = webdriver.PhantomJS()
        # self.login(username,password)
        self.browser.get('https://www.linkedin.com/')
        username_field = self.browser.find_element_by_id("login-email")
        password_field = self.browser.find_element_by_id("login-password")
        username_field.send_keys(username)
        password_field.send_keys(password)
        self.browser.find_element_by_name("submit").click()

    def build_search_url(self,search_dic,search_type='person'):
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
        url = ''
        if search_type == 'person':
            url = 'https://www.linkedin.com/vsearch/p?'+search_string
        elif search_type == 'company':
            url = 'https://www.linkedin.com/vsearch/c?'+search_string+'&f_CCR=us%3A0,in%3A0&f_CS=C,D,E,F,G,H,I&f_I=96,4,6,109,118,5,24,147,132'
        elif search_type == 'jobs':
            url = 'https://www.linkedin.com/vsearch/j?'+search_string
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
                    # if 'result' in result_class and 'people' in result_class:
                    if 'result' in result_class :
                        result_dic = {}
                        try:
                            result_dic['Name'] = result.find('div',{'class':'bd'}).find('h3').find('a',{'class':'title main-headline'}).getText(' ')
                        except:
                            pass
                        try:
                            result_dic['url'] = result.find('div',{'class':'bd'}).find('h3').find('a',{'class':'title main-headline'})['href']
                        except:
                            pass
                        try:
                            result_dic['Description'] = result.find('div',{'class':'bd'}).find('div',{'class':'description'}).getText(' ')
                        except:
                            pass
                        try:
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
                        except:
                            pass
                        if result_dic:
                            out_list.append(result_dic)
                    else:
                        continue
                except:
                    continue
        else:
            raise ValueError('The page does not have results')
        return out_list

    def get_all_details_from_searchpage(self,page):
        ''' take a linkedin search results page as input, crawl the page and return results
        :param page: page text
        :return:
        '''
        if type(page) == unicode:
            page = str(page.encode('utf-8'))
        soup = BeautifulSoup(page)
        out_list = self.get_all_details_from_search_soup(soup)
        return out_list

    def get_all_results(self,search_dic,search_type='person',max_page=10):
        ''' fetch results from each page
        :param search_dic:
        :return:
        '''
        self.max_pages = max_page
        page_no = 1
        out_list = []
        while page_no<=self.max_pages:
            try:
                # pdb.set_trace()
                print('page no:',page_no)
                search_dic['page_num'] = [str(page_no)]
                url = self.build_search_url(search_dic,search_type=search_type)
                text = self.get_url(url)
                out_list.extend(self.get_all_details_from_searchpage(text))
            except Exception as e:
                print(e)
                break
            page_no += 1
        return out_list

    def search_company(self,company_name_list=[],initial_dic={},max_page=10):
        '''
        :param company_name_list:
        :param max_page:
        :return:
        '''
        if company_name_list:
            initial_dic['keywords'] = company_name_list
            return self.get_all_results(initial_dic,search_type='company',max_page=max_page)
        else:
            return []