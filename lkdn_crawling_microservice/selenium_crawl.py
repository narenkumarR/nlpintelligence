__author__ = 'joswin'

import logging
from bs4 import BeautifulSoup
from selenium import webdriver
from pyvirtualdisplay import Display
import httplib
import socket
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary

from selenium.webdriver.remote.command import Command
# from selenium.common.exceptions import TimeoutException
from constants import firefox_binary_loc
import os

def get_status(driver):
    try:
        driver.execute(Command.STATUS)
        return "Alive"
    except (socket.error, httplib.CannotSendRequest):
        return "Dead"
    except:
        return "Dead"

class SeleniumParser(object):
    def __init__(self,browser = 'Firefox',browser_loc='/home/madan/Downloads/phantomjs-2.1.1-linux-x86_64/bin/phantomjs',
                 visible = False,proxy = False,proxy_ip=None,proxy_port=None,page_load_timeout=80,use_tor=False):
        '''Note: This module was developed to work with multiple browsers, but as of now works properly only for firefox
        :param browser: browser name {'Firefox','PhantomJS'}
        :param browser_loc: location of browser. for firefox this is not needed generally
        :param visible: should this be visible or not
        :param proxy: use any proxy ips
        :param proxy_ip: if proxy is True, use this ip
        :param proxy_port: if proxy is True, use this port
        :param page_load_timeout: waiting time for page load
        :param use_tor: flag to use tor. Tor should be started before running this code.
        :return:
        '''
        # self.visible = visible
        if not visible:
            self.display = Display(visible=0, size=(800, 600))
            self.display.start()
            # self.xvfb_pid = self.display.pid
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
                        firefox_profile.set_preference("network.proxy.ssl", proxy_ip)
                        firefox_profile.set_preference("network.proxy.ssl_port", int(proxy_port))
                        firefox_profile.set_preference("network.proxy.socks", proxy_ip)
                        firefox_profile.set_preference("network.proxy.socks_port", int(proxy_port))
                        # firefox_profile.set_preference("network.proxy.ftp", proxy_ip)
                        # firefox_profile.set_preference("network.proxy.ftp_port", int(proxy_port))
                        firefox_profile.update_preferences()
                self.browser = webdriver.Firefox(firefox_binary=FirefoxBinary(firefox_binary_loc),firefox_profile=firefox_profile)
            else:
                firefox_profile.set_preference( "network.proxy.type", 1 )
                firefox_profile.set_preference( "network.proxy.socks_version", 5 )
                firefox_profile.set_preference( "network.proxy.socks", '127.0.0.1' )
                firefox_profile.set_preference( "network.proxy.socks_port", 9050 )
                firefox_profile.set_preference( "network.proxy.socks_remote_dns", True )
                self.browser = webdriver.Firefox(firefox_binary=FirefoxBinary(firefox_binary_loc),firefox_profile=firefox_profile)
        self.browser.set_page_load_timeout(page_load_timeout)
        self.pid = self.browser.binary.process.pid
        logging.info('selenium crawl: browser started. pid : {}'.format(self.pid))

    def get_url(self,url):
        '''
        :param url:
        :return:
        '''

        try:
            self.browser.get(url)
        except:
            try:
                self.browser.execute_script("window.stop();")
            except:
                pass
            pass
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

    def get_current_page(self):
        ''' get current page in the browser(without loading, useful in javascript loading)
        :return:
        '''
        html = self.browser.page_source
        html = str(html.encode('utf-8'))
        return html

    def get_soup_current_page(self):
        ''' get soup from current page in the browser
        :return:
        '''
        html = self.get_current_page()
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
        try:
            logging.info('selenium_crawl: trying to kill firefox : pid:{}'.format(self.pid))
            os.system('kill -9 {}'.format(self.pid))
        except:
            pass
