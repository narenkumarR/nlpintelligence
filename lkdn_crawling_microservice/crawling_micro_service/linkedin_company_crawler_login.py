"""
File : linkedin_organization_service.py
Created On: 23-Sep-2015
Author: ideas2it
"""
import re
import time
from bs_crawl import BeautifulsoupCrawl
# import linkedin_parser
import selenium_crawl
import logging
from random import randint
from selenium.common.exceptions import TimeoutException
from httplib import CannotSendRequest,BadStatusLine
from socket import error as socket_error

from linkedin_login import login_fun
from constants import desig_list

all_org_cases = ['Specialties','Website','Industry','Type','Headquarters','Company Size','Founded',
                     'Company Name','Description Text','Employee Details','Also Viewed Companies','Linkedin URL']


def dec_fun(fn):
    '''
    not used
    if fn executions into error, None is returned
    :param fn:
    :return:
    '''
    def new_fun(*args,**kwargs):
        try:
            return fn(*args,**kwargs)
        except Exception as e:
            logging.exception('Exception while fetching details from company page ')
            return None
    return new_fun

def complete_cases_org(fn):
    '''
    not used
    :param fn:
    :return:
    '''
    def new_fun1(*args,**kwargs):
        try:
            out = fn(*args,**kwargs)
            out1 = {}
            for i in all_org_cases:
                try:
                    out1[i] = out[i]
                except:
                    out1[i] = ''
            return out1
        except Exception as e:
            logging.exception('Exception while running main from company page ')
            return None
    return new_fun1

def is_number(s):
    try:
        int(s)
        return True
    except ValueError:
        return False

class LinkedinOrganizationService(object):
    def __init__(self,browser='Firefox',visible = True,proxy=False,proxy_ip = None,proxy_port = None,use_tor=None,
                 login=True):
        ''' support for methods other than selenium needs fixes
        :param browser:
        :param visible:
        :param proxy:
        :param proxy_ip:
        :param proxy_port:
        :param use_tor:
        :return:
        '''
        # print('class intializing')
        self._crawler = BeautifulsoupCrawl.get_soup
        self.browser = browser
        self.visible = visible
        self.proxy = proxy
        self.proxy_ip = proxy_ip
        self.proxy_port = proxy_port
        self.use_tor = use_tor
        self.login = login
        if self.browser:
            self.init_selenium_parser(browser,visible,proxy,proxy_ip,proxy_port,use_tor,login)

    def exit(self):
        self.link_parser.exit()

    def init_selenium_parser(self,browser=None,visible = None,proxy=None,proxy_ip = None,proxy_port = None,
                             use_tor=None,login=False):
        '''
        :param browser:
        :param visible:
        :param proxy:
        :param proxy_ip:
        :param proxy_port:
        :param use_tor:
        :param login:
        :return:
        '''
        if browser is None and visible is None and visible is None and proxy is None and use_tor is None: #if no input reload same parser
            # self.link_parser = linkedin_parser.LinkedinParserSelenium(self.browser,visible=self.visible,proxy=self.proxy,
            #                                                           proxy_ip=self.proxy_ip,proxy_port=self.proxy_port,
            #                                                           use_tor=self.use_tor)
            self.link_parser = selenium_crawl.SeleniumParser(self.browser,visible=self.visible,proxy=self.proxy,
                                                                      proxy_ip=self.proxy_ip,proxy_port=self.proxy_port,
                                                                      use_tor=self.use_tor)
        else:
            try:  # try with inputs. if success, update the class variables. if error, try with already existing class vars
                # self.link_parser = linkedin_parser.LinkedinParserSelenium(browser,visible=visible,proxy=proxy,
                #                                                           proxy_ip=proxy_ip,proxy_port=proxy_port,use_tor=use_tor)
                self.link_parser = selenium_crawl.SeleniumParser(browser,visible=visible,proxy=proxy,
                                                                          proxy_ip=proxy_ip,proxy_port=proxy_port,use_tor=use_tor)
                self.browser = browser
                self.visible = visible
                self.proxy = proxy
                self.proxy_ip = proxy_ip
                self.proxy_port = proxy_port
                self.use_tor = use_tor
            except:
                # self.link_parser = linkedin_parser.LinkedinParserSelenium(self.browser,visible=self.visible,proxy=self.proxy,
                #                                                   proxy_ip=self.proxy_ip,proxy_port=self.proxy_port,use_tor=self.use_tor)
                self.link_parser = selenium_crawl.SeleniumParser(self.browser,visible=self.visible,proxy=self.proxy,
                                                                  proxy_ip=self.proxy_ip,proxy_port=self.proxy_port,use_tor=self.use_tor)
        if login:
            login_fun(self.link_parser.browser)

    # @complete_cases_org
    def get_organization_details_from_linkedin_link(self,url,use_selenium=True,designations=[],next_page=2):
        '''
        :param url: linkedin company url
        :return:
        '''
        details = {} #to prevent errors in the last line of this function
        try:
            if use_selenium:
                soup = self.link_parser.get_soup(url)
                redirect_url = self.link_parser.browser.current_url
                if '?trk=login_reg_redirect' in redirect_url:
                    redirect_url = url
            else:
                soup = self._crawler(url)
                redirect_url = url
                # logging.info(self.soup)
            # else:
            #     self.details['Notes'] = 'Publicaly available'
            details['Linkedin URL'] = re.split('\?trk',redirect_url)[0]
            details['Original URL'] = url
            self.fetch_details_soupinput(soup,designations,next_page,details)
            return details
        except TimeoutException:
            logging.error('Time out exception for url: '+url)
            return None
        except socket_error:
            logging.error('Socket error for url:'+url)
            return None
        except CannotSendRequest:
            logging.error('Cannot send request error for url:{}'.format(url))
            return None
        except BadStatusLine:
            logging.error('Badstatus line error for url:{}'.format(url))
            return None
        except Exception as e:
            logging.exception('Exception while running main from company page for url:'+url)
            return details

    def fetch_details_soupinput(self,soup,designations=[],next_page=3,details={}):
        ''' from soup object, get details. Note that this function requires a browser logged into Linkedin as link_parser
        because for getting people, it uses browser to search.
        :param soup:
        :param designations:
        :param next_page:
        :return:
        '''
        if not designations:
            designations = desig_list
        if re.search(r'the company you are looking for is not active|the company you are looking for does not exist',soup.text):
            details['Notes'] = 'Company page not found'
            return details
        try:
            if 'Largest Professional Network' in soup.title.text:
                details['Notes'] = 'Not Available Pubicly'
                return details
        except:
            details['Notes'] = 'Java script code'
            return details
        details['Company Name'] = self.get_name(soup)
        details['Description Text'] = self.get_description(soup)
        self.get_details(soup,details)
        details['Also Viewed Companies'] = self.get_also_viewed(soup)
        details['Employee count Linkedin'] = self.get_linkedin_employees_count(soup)
        details['Employee Details'] = self.get_employees(soup,designations,next_page,url = details['Linkedin URL'])
        return details

    # @dec_fun
    def get_description(self,soup):
        '''
        :return:
        '''
        try:
            para = soup.find("div", {"class": "basic-info-description"}).find('p').getText()
            para = re.sub(r'[.]+','. ',re.sub(r'[\r\n]','.',para))
            return para
        except:
            return ''

    # @dec_fun
    def get_name(self,soup):
        '''
        :return:
        '''
        try:
            return soup.find("div",{"class":"content-wrapper"}).find("h1").find("span").getText().strip()
        except:
            return ''

    # @dec_fun
    def get_details(self,soup,details = {}):
        '''
        :param soup:
        :param details:
        :return:
        '''
        try:
            headers = ['h3','h4']
            for header in headers:
                for tag in soup.find("div",{"class":"basic-info-about"}).findAll(header):
                    try:
                        details[tag.getText().strip()] = tag.find_next().getText().strip()
                    except:
                        continue
        except:
            return

    def get_linkedin_employees_count(self,soup):
        try:
            linkedin_emp_count = soup.find('div', {'id': 'stream-right-rail'}).find('div', {
               'class': 'how-connected'})
        except:
            try:
               logging.info('company_part:employee_details_in_linkedin_count went to exception. try the logging option')
               linkedin_emp_count = soup.find('div', {'class': 'how-connected'})
               # logging.info('plist len loggin option 1 {}'.format(len(linkedin_emp_count)))
            except:
               logging.info('went to exception while trying loggin')
               # self.details['Employee count Linkedin'] = ''
               return ''
        if not linkedin_emp_count:
            logging.info('no p_list. try logging')
            try:
               linkedin_emp_count = soup.find('div', {'class': 'how-connected'})
               logging.info('p list logging opiton :{}'.format(len(linkedin_emp_count)))
            except:
               logging.info('went to exception while trying loggin 2')
               # self.details['Employee count Linkedin'] = ''
               return ''
        if not linkedin_emp_count:
            # self.details['Employee count Linkedin'] = ''
            return ''
        # out_list = []
        # for tmp in linkedin_emp_count:
        try:
            linkedin_emp_count_list = linkedin_emp_count.find('ul', {'class': 'stats'}).find_all('li')
            for emp_count in linkedin_emp_count_list:
                if 'Employees on LinkedIn' in emp_count.find('span').text.strip():
                    # self.details['Employee count Linkedin'] = ''
                    return emp_count.find('a').text.strip()

                # else:
                #     # self.details['Employee count Linkedin'] = linkedin_emp_count_list[1].find('a').text.strip()
                #     return ''
        except:
            # self.details['Employee count Linkedin'] = ''
           # logging.info('out_list : {}'.format(out_list))
            return ''
        # return ''

    # @dec_fun
    def get_employees(self,soup,designations,next_page,url):
        '''
        :return:
        '''
        # logging.info('get_employees started')
        # if url is ending with a number like (linkedin.com/company/1234), we can construct the employee url ourselves
        url_last_part = False
        try:
            linkedin_emp_see_all_link = soup.find('div', {'class': 'stream-right-rail'}).find('div', {
                'class': 'how-connected'})
            # logging.info('plist length:{}'.format(len(p_list)))
        except:
            try:
                logging.info(
                    'company_part:employee_details_in_linkedin_count went to exception. try the logging option')
                linkedin_emp_see_all_link = soup.find('div', {'class': 'how-connected'})
                logging.info('plist len loggin option 1 {}'.format(len(linkedin_emp_see_all_link)))
            except:
                logging.info('went to exception while trying url construction')
                try:
                    url_last_part = re.split(r'/company/|/company-beta/',url.split('?')[0])[1]
                except:
                    return []
                if is_number(url_last_part):
                    url_last_part = int(url_last_part)
                else:
                    return []
        if not linkedin_emp_see_all_link:
            logging.info('no p_list. try logging')
            try:
                linkedin_emp_see_all_link = soup.find('div', {'class': 'how-connected'})
                logging.info('p list logging opiton :{}'.format(len(linkedin_emp_see_all_link)))
            except:
                logging.info('went to exception while trying loggin 2')
                # self.details['Employee Details'] = []
                return []
        out_list = []
        if linkedin_emp_see_all_link: #if true, link could be extracted from page
            next_link=linkedin_emp_see_all_link.find('a')['href']
        elif url_last_part:#generate next link using url_last_part if it is available
            next_link = 'https://www.linkedin.com/vsearch/p?f_CC={}'.format(url_last_part)
        if next_link.startswith('/vsearch'):
            next_link = 'https://www.linkedin.com'+next_link
        # query_part=next_link+'&title=' +' OR '.join(designations)+'&page_num=2'
        time.sleep(randint(5,8))
        for tmp in range(1,next_page+1):
            query_part = next_link + '&title=' + ' OR '.join(designations) + '&page_num='+str(tmp) #&titleScope=C (only current, not used)
            soup = self.link_parser.get_soup(query_part)
            time.sleep(randint(5,8))
            if 'Sorry, no results containing all your search terms were found.' in soup.text:
                break
            people_information=soup.find('div', {'id': 'results-col'}).find('div', {
                'id': 'results-container'}).find('ol').findAll('li')
            for people_complete_info in people_information:
                tmp_dic = {}
                try:
                    tmp_dic['linkedin_url'] = people_complete_info.find('div', {'class': 'bd'}).find('h3').find('a')['href']
                except:
                    tmp_dic['linkedin_url'] = ''
                try:
                    tmp_dic['Name'] = people_complete_info.find('div', {'class': 'bd'}).find('h3').find('a').text.strip()
                except:
                    tmp_dic['Name'] = ''
                try:
                    tmp_dic['Designation'] = people_complete_info.find('div', {'class': 'bd'}).find('div', {'class': 'description'}).text
                except:
                    tmp_dic['Designation'] = ''
                try:
                    tmp_dic['Industry'] = people_complete_info.find('div', {'class': 'bd'}).find('dl', {'class': 'demographic'}).find('dd').text
                except:
                    tmp_dic['Industry'] = ''
                try:
                    tmp_dic['Location'] = people_complete_info.find('div', {'class': 'bd'}).find('dl', {'class': 'demographic'}).find('dd', {'class': 'separator'}).text
                except:
                    tmp_dic['Location'] = ''
                try:
                    tmp_dic['current_company'] = people_complete_info.find('div', {'class': 'bd'}).find('dl', {'class': 'snippet'}).find('dd').find('p').text
                except:
                    tmp_dic['current_company'] = ''
                out_list.append(tmp_dic)
            try: #find no of employees in the company. if no/10 (eg, 15/10 -> 1, and page_no =2, stop process)
                res_count_str = re.sub(',','',soup.find('div',{'id':'results_count'}).find('strong').text)
                res_count = int(res_count_str)
                if res_count/10 + 1 == tmp:
                    break
            except:
                continue
            # logging.info('out_list : {}'.format(out_list))
        # self.details['Employee Details'] = out_list
        return out_list

    def get_also_viewed(self,soup):
        '''
        :return:
        '''
        try:
            p_list = soup.find('div',{'class':'also-viewed module'}).find('ul').findAll('li')
        except:
            # details['Also Viewed Companies'] = []
            return []
        out_list = []
        for tmp in p_list:
            tmp_dic = {}
            try:
                tmp_dic['company_linkedin_url'] = tmp.find('a')['href']
            except:
                tmp_dic['company_linkedin_url'] = ''
            try:
                tmp_dic['Company Name'] = tmp.find('a').find('img')['alt']
            except:
                tmp_dic['Company Name'] = ''
            out_list.append(tmp_dic)
        return out_list