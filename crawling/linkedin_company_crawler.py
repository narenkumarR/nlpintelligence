"""
File : linkedin_organization_service.py
Created On: 23-Sep-2015
Author: ideas2it
"""
import re
from bs_crawl import BeautifulsoupCrawl
# import linkedin_parser
import selenium_crawl
import logging
from selenium.common.exceptions import TimeoutException
from httplib import CannotSendRequest,BadStatusLine
from socket import error as socket_error

all_org_cases = ['Specialties','Website','Industry','Type','Headquarters','Company Size','Founded',
                     'Company Name','Description Text','Employee Details','Also Viewed Companies','Linkedin URL']


def dec_fun(fn):
    '''
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



class LinkedinOrganizationService(object):
    def __init__(self,browser='Firefox',visible = True,proxy=False,proxy_ip = None,proxy_port = None,use_tor=None):
        # print('class intializing')
        self._crawler = BeautifulsoupCrawl.single_wp
        self.browser = browser
        self.visible = visible
        self.proxy = proxy
        self.proxy_ip = proxy_ip
        self.proxy_port = proxy_port
        self.use_tor = use_tor
        self.init_selenium_parser(browser,visible,proxy,proxy_ip,proxy_port,use_tor)

    def exit(self):
        self.link_parser.exit()

    def init_selenium_parser(self,browser=None,visible = None,proxy=None,proxy_ip = None,proxy_port = None,use_tor=None):
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

    # @complete_cases_org
    def get_organization_details_from_linkedin_link(self,url,use_selenium=True):
        '''
        :param url: linkedin company url
        :return:
        '''
        try:
            self.details = {'Linkedin URL':url,'Original URL':url}
            if use_selenium:
                self.soup = self.link_parser.get_soup(url)
                redirect_url = self.link_parser.browser.current_url
                if '?trk=login_reg_redirect' in redirect_url:
                    redirect_url = url
            else:
                self.soup = self._crawler(url)
                redirect_url = url
            try:
                if 'Largest Professional Network' in self.soup.title.text:
                    self.details['Notes'] = 'Not Available Pubicly'
                    return self.details
            except:
                self.details['Notes'] = 'Java script code'
                return self.details
                # logging.info(self.soup)
            # else:
            #     self.details['Notes'] = 'Publicaly available'
            self.details['Linkedin URL'] = redirect_url
            self.get_name()
            self.get_description()
            self.get_details()
            self.get_employees()
            self.get_also_viewed()
            return self.details
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
            return self.details

    # @dec_fun
    def get_description(self):
        '''
        :return:
        '''
        try:
            para = self.soup.find("div", {"class": "basic-info-description"}).findChild().getText()
            para = re.sub(r'[.]+','. ',re.sub(r'[\r\n]','.',para))
            self.details['Description Text'] = para
        except:
            self.details['Description Text'] = ''

    # @dec_fun
    def get_name(self):
        '''
        :return:
        '''
        try:
            self.details['Company Name'] = self.soup.find("div",{"class":"content-wrapper"}).find("h1").findChild().getText().strip()
        except:
            return

    # @dec_fun
    def get_details(self):
        '''
        :return:
        '''
        try:
            headers = ['h3','h4']
            for header in headers:
                for tag in self.soup.find("div",{"class":"basic-info-about"}).findAll(header):
                    try:
                        self.details[tag.getText().strip()] = tag.find_next().getText().strip()
                    except:
                        continue
        except:
            return
    # @dec_fun
    def get_employees(self):
        '''
        :return:
        '''
        try:
            p_list = self.soup.find('div',{'class':'company-employees module'}).findAll('li')
        except:
            self.details['Employee Details'] = []
            return
        out_list = []
        for tmp in p_list:
            tmp_dic = {}
            try:
                tmp_dic['linkedin_url'] = tmp.find('a')['href']
            except:
                tmp_dic['linkedin_url'] = ''
            try:
                tmp_dic['Name'] = tmp.find('dt').text
            except:
                tmp_dic['Name'] = ''
            try:
                tmp_dic['Designation'] = tmp.find('dd').text
            except:
                tmp_dic['Designation'] = ''
            out_list.append(tmp_dic)
        self.details['Employee Details'] = out_list

    # @dec_fun
    def get_also_viewed(self):
        '''
        :return:
        '''
        try:
            p_list = self.soup.find('div',{'class':'also-viewed module'}).findAll('li')
        except:
            self.details['Also Viewed Companies'] = []
            return
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
        self.details['Also Viewed Companies'] = out_list