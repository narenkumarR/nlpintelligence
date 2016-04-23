__author__ = 'joswin'

from selenium_crawl import SeleniumParser
import logging

class ProxyGen(object):
    '''
    '''
    def __init__(self,browser='Firefox',visible=True,page_load_timeout=60):
        self.browser = browser
        self.visible = visible
        self.page_load_timeout = page_load_timeout
        self.activate_browser()

    def activate_browser(self):
        self.browser = SeleniumParser(browser=self.browser,visible=self.visible,
                                      page_load_timeout=self.page_load_timeout)
        self.browser_active = True

    def generate_proxy(self):
        '''
        :return:
        '''
        if not self.browser_active:
            self.activate_browser()
        proxy_list = self.get_proxy_ultraproxies()
        return proxy_list

    def exit(self):
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
