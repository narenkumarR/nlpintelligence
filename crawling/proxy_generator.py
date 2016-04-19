__author__ = 'joswin'

from selenium_crawl import SeleniumParser

class ProxyGen(object):
    '''
    '''
    def __init__(self,visible=True,page_load_timeout=25):
        self.browser = SeleniumParser(visible=visible,page_load_timeout=page_load_timeout)

    def generate_proxy(self):
        '''
        :return:
        '''
        pass

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
