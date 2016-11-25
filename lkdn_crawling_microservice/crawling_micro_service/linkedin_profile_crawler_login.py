"""
File : linkedin_profile_crawler.py
Created On: 07-Mar-2016
Author: ideas2it
"""
import re
from bs_crawl import BeautifulsoupCrawl
# import linkedin_parser
import selenium_crawl
# from bs4 import BeautifulSoup
from selenium.common.exceptions import TimeoutException
from socket import error as socket_error
from httplib import CannotSendRequest,BadStatusLine

from linkedin_login import login_fun

import logging
# logger = logging.getLogger(__name__)

class LinkedinProfileCrawler(object):
    '''Crawl a linkedin profie page
    '''
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

    def init_selenium_parser(self,browser=None,visible = None,proxy=None,proxy_ip = None,proxy_port = None,use_tor=None,
                             login=False):
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
                #                                                           proxy_ip=self.proxy_ip,proxy_port=self.proxy_port,
                #                                                           use_tor=self.use_tor)
                self.link_parser = selenium_crawl.SeleniumParser(self.browser,visible=self.visible,proxy=self.proxy,
                                                                          proxy_ip=self.proxy_ip,proxy_port=self.proxy_port,
                                                                          use_tor=self.use_tor)
        if login:
            login_fun(self.link_parser.browser)

    def fetch_details_urlinput(self,url,use_selenium = True):
        '''
        :param url:
        :param outs_needed: list of parameters need to be fetched. if empty, all are fetched
        :return: dictionary with the fetched details
        '''
        outs = {'Linkedin URL':url,'Original URL':url}
        if '/pub/' in url:
            url = re.sub('/pub/','/in/','/'.join(url.split('/')[:-3])+'-'+''.join(url.split('/')[-3:][::-1]))
        try:
            if use_selenium:
                soup = self.link_parser.get_soup(url)
                redirect_url = self.link_parser.browser.current_url
                if '?trk=login_reg_redirect' in redirect_url:
                    redirect_url = url
            else:
                soup = self._crawler(url)
                redirect_url = url
            try:
                if 'Largest Professional Network' in soup.title.text:
                    outs['Notes'] = 'Not Available Pubicly'
                    return outs
            except:
                outs['Notes'] = 'Java script code'
                return outs
            outs['Linkedin URL'] = redirect_url
            tmp = self.fetch_details_soupinput(soup)
            outs.update(tmp)
        except TimeoutException:
            logging.error('Time out exception for url:'+url)
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
            logging.exception('Error while fetching details for url: '+url)
        return outs

    def fetch_details_soupinput(self,soup):
        '''
        :param soup:
        :param outs_needed: list of parameters need to be fetched. if empty, all are fetched
        :return: dictionary with the fetched details
        '''
        # if not outs_needed:
        #     outs_needed = ['Name','Position','Company','CompanyLinkedinPage','Location','PreviousCompanies','Education',
        #                    'Industry']
        outs = {}
        outs['Name'] = self.get_name(soup)
        outs['Position'] = self.get_position(soup)
        outs['Company'] = self.get_company(soup)
        outs['Location'] = self.get_location(soup)
        outs['CompanyLinkedinPage'] = self.get_company_linkedin_page(soup)
        outs['PreviousCompanies'] = self.get_previous_companies(soup)
        outs['Education'] = self.get_education(soup)
        outs['Industry'] = self.get_industry(soup)
        outs['Summary'] = self.get_summary(soup)
        outs['Experience'] = self.get_experience(soup)
        outs['Skills'] = self.get_skills(soup)
        outs['Related People'] = self.get_related_people(soup)
        outs['Same Name People'] = self.get_same_name_people(soup)
        return outs

    def get_name(self,soup):
        '''
        :param soup:
        :return:
        '''
        try:
            self.person_name=soup.find('div',{'id':'top-card'}).find('div',{'id':'name'}).find('h1').find('span',{'class':'full-name'}).text

            return self.person_name
        except:
            return ''

    def get_position(self,soup):
        '''
        :param soup:
        :return:
        '''
        try:
            return soup.find('div',{'id':'top-card'}).find('div',{'id':'headline'})\
                .find('p',{'class':'title'}).text
        except:
            return ''

    def get_company(self,soup):
        '''
        :param soup:
        :return:
        '''
        try:
            tmp = soup.find('div',{'id':'background'}).find('div',{'id':'background-experience-container'}). \
                find('div', {'id': 'background-experience'}).findAll('div')
            for company_list in tmp:
                cmp_dic = {}
                try:
                    company_name = company_list.find('span',{'data-tracking':'mcp_profile_sum'}).find('a').text
                    return '|'.join(company_name)
                except:
                    return ''

            # names = [i.find('a').text for i in tmp]

        except:
            return ''

    def get_company_linkedin_page(self,soup):
        '''
        :param soup:
        :return:
        '''
        try:
            tmp = soup.find('div',{'id':'background'}).find('div',{'id':'background-experience-container'}). \
                find('div', {'id': 'background-experience'}).findAll('div')
            for company_list in tmp:
                cmp_dic = {}
                try:
                    company_linkedin_page = company_list.find('span',{'data-tracking':'mcp_profile_sum'}).find('a')['href']
                    return '|'.join(company_linkedin_page)
                except:
                    return ''

            # names = [i.find('a').text for i in tmp]

        except:
            return ''

    def get_location(self,soup):
        '''
        :param soup:
        :return:
        '''
        try:
            return soup.find('div',{'id':'top-card'}).find('div',{'id':'location'})\
                .find('span',{'class':'locality'}).text
        except:
            return ''

    def get_industry(self,soup):
        try:
            tmp = soup.find('div',{'id':'top-card'}).find('div',{'id':'location'})\
                .find('dd',{'class':'industry'})
            # if len(tmp)>1:
            return tmp.text
        except:
            return ''

    def get_previous_companies(self,soup):
        '''
        :param soup:
        :return:
        '''
        try:
            tmp= soup.find('div',{'id':'top-card'}).find('div',{'class':'profile-overview-content'})\
                .find('table',{'class':'Overview for'+self.person_name}).find('tr',{'id':'overview-summary-past'}).find('td').find('ol').findAll('li')
            cmp_names = [i.find('a').text for i in tmp]
            return '|'.join(cmp_names)
        except:
            return ''

    def get_education(self,soup):
        '''
        :param soup:
        :return:
        '''
        out_list = []
        edu_dic = {}
        try:

            edu_text= soup.find('div',{'id':'background'}).find('div',{'id':'background-education-container'}). \
                find('div', {'id': 'background-education'}).findAll('div')
            for edu_info in edu_text:
                edu_dic = {}
                try:
                    edu_dic['institute_name']=edu_info.find('h4').text
                except:
                    edu_dic['institute_name']= ''
                try:
                    edu_dic['degree']=edu_info.find('h5').find('span',{'class':'degree'}).text
                except:
                    edu_dic['degree']= ''
                out_list.append(edu_dic)
            return out_list

        except:
            return []


    def get_summary(self,soup):
        try:
            return soup.find('div',{'id':'background'}).find('div',{'class':'summary'}).text
        except:
            return ''

    def get_experience(self,soup):
        try:
            out_list = []
            # exps = soup.find('section',{'id':'experience'}).findAll('li')
            exps = soup.find('div', {'id': 'background'}).find('div', {'class': 'background-content'}).find('div', {
                'id': 'background-experience-container'}).find('div',{'id':'background-experience'}).findAll('div')
            for exp in exps:
                exp_dic = {}
                try:
                    exp_dic['Position'] = exp.find('h4').find('a').text.strip()
                except:
                    # continue  # if no position, continue
                    exp_dic['Position'] = ''
                try:
                    exp_dic['Company'] = exp.find('h5').find('a').text.strip()
                except:
                    continue # if no company, continue
                    # exp_dic['Company'] = ''
                try:
                    exp_dic['Company Linkedin'] = exp.find('h5').find('a')['href']
                except:
                    exp_dic['Company Linkedin'] = ''
                try:
                    exp_dic['Date Range'] = str(exp.find('span',{'class':'experience-date-locale'}).text.encode('ascii','ignore'))
                except:
                    exp_dic['Date Range'] = ''
                try:
                    exp_dic['Location'] = exp.find('span',{'class':'locality'}).text
                except:
                    exp_dic['Location'] = ''
                try:
                    exp_dic['Description'] = exp.find('p').text
                except:
                    exp_dic['Description'] = ''
                out_list.append(exp_dic)
            return out_list
        except:
            return []

    def get_skills(self,soup):
        try:
            return soup.find('div', {'id': 'background'}).find('div', {'class': 'background-content'}).find('div',{'id':'background-skills-container'}).find('div',{'id':'profile-skills'}).find('ul').getText(',')
        except:
            return ''

    def get_related_people(self,soup):
        try:
            try:
                lis = soup.find('div',{'class':'right-fixed'}).find('div',{'class':'discovery-panel'}).find('ol').findAll('li')
            except: #new structure
                lis = soup.find('div',{'class':'insights-browse-map'}).find('ol').findAll('li')
            out_list = []
            for li in lis:
                li_dic = {}
                try:
                    li_dic['Linkedin Page'] = li.find('dl',{'class':'class="discovery-detail"'}).find('a')['href']
                except:
                    li_dic['Linkedin Page'] = ''
                try:
                    li_dic['Name'] =li.find('dl',{'class':'class="discovery-detail"'}).find('a').text
                except:
                    continue # if no name continue
                    # li_dic['Name'] = ''
                try:
                    li_dic['Position'] = li.find('dl',{'class':'class="discovery-detail"'}).find('dd').text
                except:
                    li_dic['Position'] = ''
                out_list.append(li_dic)
            return out_list
        except:
            return []

    def get_same_name_people(self,soup):
        try:
            try:
                lis = soup.find('div',{'class':'insights-browse-map'}).find('ul',{'class':'browse-map-list'}).findAll('li')
            except:
                lis = soup.find('div',{'ul':'browse-map-list'}).findAll('li')
            out_list = []
            for li in lis:
                li_dic = {}
                try:
                    li_dic['Linkedin Page'] = li.find('h4').find('a')['href']
                except:
                    li_dic['Linkedin Page'] = ''
                try:
                    li_dic['Name'] = li.find('h4').find('a').text
                except:
                    continue # if no name continue
                    # li_dic['Name'] = ''
                try:
                    li_dic['Position'] = li.find('p').text
                except:
                    li_dic['Position'] = ''
                out_list.append(li_dic)
            return out_list
        except:
            return []