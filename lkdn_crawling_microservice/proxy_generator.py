__author__ = 'joswin'

from selenium_crawl import SeleniumParser
from bs_crawl import BeautifulsoupCrawl
import logging
from random import shuffle

class ProxyGen(object):
    '''
    '''
    def __init__(self,browser='Firefox',visible=True,page_load_timeout=60,parser='selenium'):
        self.browser = browser
        self.visible = visible
        self.page_load_timeout = page_load_timeout
        self.parser = parser
        # self.activate_browser()

    def activate_browser(self):
        if self.parser == 'selenium':
            self.browser = SeleniumParser(browser=self.browser,visible=self.visible,
                                          page_load_timeout=self.page_load_timeout)
            self.browser_active = True
        else:
            self.browser = BeautifulsoupCrawl

    def generate_proxy(self):
        '''
        :return:
        '''
        if not self.browser_active:
            self.activate_browser()
        proxy_list = []
        # try:
        #     proxy_list.extend(self.get_proxy_ultraproxies())
        # except:
        #     pass
        try:
            proxy_list.extend(self.get_proxy_free_proxy_list())
        except:
            pass
        try:
            proxy_list.extend(self.get_proxy_proxy_list_org())
        except:
            pass
        shuffle(proxy_list)
        return proxy_list

    def exit(self):
        if self.parser == 'selenium':
            if self.browser_active:
                logging.info('exiting proxy browser')
                self.browser.exit()
                self.browser_active = False
                logging.info('finished exiting proxy browser')

    def gen_proxy_samair(self):
        ''' look at proxy from http://www.samair.ru/proxy/ip-address-01.htm
        :return:
        '''
        # soup = bsc.single_wp('http://www.samair.ru/proxy/ip-address-01.htm')
        # trs = soup.find('table',{'id':'proxylist'}).findAll('tr')
        # for tr in trs[1:]:
        pass

    def get_proxy_ultraproxies(self):
        ''' from http://www.ultraproxies.com/
        :return:
        '''
        soup = self.browser.get_soup('http://www.ultraproxies.com/')
        tables = soup.findAll('table')
        table = tables[5]
        trs = table.findAll('tr')[1:]
        proxy_list = []
        for tr in trs:
            ip = tr.findAll('td')[0].text[:-1]
            port = trs[0].findAll('td')[1].text
            proxy_list.append((ip,port))
        return proxy_list

    def get_proxy_free_proxy_list(self):#will work only with selenium
        soup = self.browser.get_soup('http://free-proxy-list.net/')
        proxy_list = []
        tmp = soup.find('div',{'class':'block-settings'}).find('div',{'id':'proxylisttable_wrapper','class':'dataTables_wrapper'}).find('tbody').findAll('tr')
        for tr in tmp:
            if tr.findAll('td')[6].text == 'yes':
                ip = tr.findAll('td')[0].text
                port = tr.findAll('td')[1].text
                proxy_list.append((ip,port))
        return proxy_list

    def get_proxy_proxy_list_org(self):
        # soup = self.browser.get_soup('https://proxy-list.org/english/index.php')
        soup = self.browser.get_soup('https://proxy-list.org/english/search.php?search=ssl-yes&country=any&type=any&port=any&ssl=yes')
        tmp = soup.find('div',{'id':'proxy-table','class':'proxy-table'}).find('div',{'class':'table'})
        uls = tmp.findAll('ul')
        proxy_list = []
        for ul in uls:
            if ul.find('li',{'class':'https'}).text == 'HTTPS':
                ip_part = ul.find('li',{'class':'proxy'})
                chk = [s.extract() for s in ip_part]
                ip,port = ip_part.text.split(':')
                if ip and port:
                    proxy_list.append((ip,port))
        return proxy_list
