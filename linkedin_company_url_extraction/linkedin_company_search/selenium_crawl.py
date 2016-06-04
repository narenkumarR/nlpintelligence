__author__ = 'joswin'

import logging
from bs4 import BeautifulSoup
from selenium import webdriver
from pyvirtualdisplay import Display
import httplib
import socket

from selenium.webdriver.remote.command import Command

def get_status(driver):
    try:
        driver.execute(Command.STATUS)
        return "Alive"
    except (socket.error, httplib.CannotSendRequest):
        return "Dead"

class SeleniumParser(object):
    def __init__(self,browser = 'Firefox',browser_loc='/home/madan/Downloads/phantomjs-2.1.1-linux-x86_64/bin/phantomjs',
                 visible = True,proxy = False,proxy_ip=None,proxy_port=None,page_load_timeout=25,use_tor=False):
        if not visible:
            self.display = Display(visible=0, size=(800, 600))
            self.display.start()
        else:
            self.display = None
        if browser == 'PhantomJS':
            self.browser = webdriver.PhantomJS(browser_loc)
        else:
            firefox_profile = webdriver.FirefoxProfile()
            firefox_profile.set_preference("browser.privatebrowsing.autostart", True)
            firefox_profile.set_preference( "permissions.default.image", 2 )
            if not use_tor:
                if proxy:
                    if (not proxy_ip) or (not proxy_port) or (proxy_ip is None) or (proxy_port is None):
                        logging.error('No ip/port, not using proxy')
                    else:
                        firefox_profile.set_preference("network.proxy.type", 1)
                        firefox_profile.set_preference("network.proxy.http", proxy_ip)
                        firefox_profile.set_preference("network.proxy.http_port", int(proxy_port))
                        firefox_profile.update_preferences()
                self.browser = webdriver.Firefox(firefox_profile=firefox_profile)
            else:
                firefox_profile.set_preference( "network.proxy.type", 1 )
                firefox_profile.set_preference( "network.proxy.socks_version", 5 )
                firefox_profile.set_preference( "network.proxy.socks", '127.0.0.1' )
                firefox_profile.set_preference( "network.proxy.socks_port", 9050 )
                firefox_profile.set_preference( "network.proxy.socks_remote_dns", True )
                self.browser = webdriver.Firefox(firefox_profile=firefox_profile)
        self.browser.set_page_load_timeout(page_load_timeout)

    def get_url(self,url):
        '''
        :param url:
        :return:
        '''
        self.browser.get(url)
        html = self.browser.page_source
        html = str(html.encode('utf-8'))
        return html

    def get_soup(self,url):
        '''
        :param url:
        :return:
        '''
        html = self.get_url(url)
        soup = BeautifulSoup(html)
        return soup

    def exit(self):
        '''
        :return:
        '''
        # self.browser.close()
        if get_status(self.browser) == 'Alive':
            self.browser.quit()
        if self.display:
            self.display.stop()
