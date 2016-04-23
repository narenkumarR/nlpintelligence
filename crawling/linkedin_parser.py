__author__ = 'joswin'

import logging
import cookielib
import urllib
import urllib2
import os
from bs4 import BeautifulSoup
from selenium import webdriver
from pyvirtualdisplay import Display

cookie_filename = "parser.cookies.txt"

class LinkedInParserUrllib2(object):
    ''' Not working properly due to javascript problem
    '''
    def __init__(self, login, password):
        """ Start up... """
        self.login = login
        self.password = password

        # Simulate browser with cookies enabled
        self.cj = cookielib.MozillaCookieJar(cookie_filename)
        if os.access(cookie_filename, os.F_OK):
            self.cj.load()
        self.opener = urllib2.build_opener(
            urllib2.HTTPRedirectHandler(),
            urllib2.HTTPHandler(debuglevel=0),
            urllib2.HTTPSHandler(debuglevel=0),
            urllib2.HTTPCookieProcessor(self.cj)
        )
        self.opener.addheaders = [
            ('User-agent', ('Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'))
            # ('User-agent', ('Mozilla/4.0 (compatible; MSIE 6.0; '
            #                'Windows NT 5.2; .NET CLR 1.1.4322)'))
        ]

        # Login
        self.loginPage()

        title = self.loadTitle()
        print title

        self.cj.save()


    def loadPage(self, url, data=None):
        """
        Utility function to load HTML from URLs for us with hack to continue despite 404
        """
        # We'll print the url in case of infinite loop
        # print "Loading URL: %s" % url
        try:
            if data is not None:
                response = self.opener.open(url, data)
            else:
                response = self.opener.open(url)
            return ''.join(response.readlines())
        except:
            # If URL doesn't load for ANY reason, try again...
            # Quick and dirty solution for 404 returns because of network problems
            # However, this could infinite loop if there's an actual problem
            return self.loadPage(url, data)

    def loadSoup(self, url, data=None):
        """
        Combine loading of URL, HTML, and parsing with BeautifulSoup
        """
        html = self.loadPage(url, data)
        soup = BeautifulSoup(html, "html5lib")
        return soup

    def loginPage(self):
        """
        Handle login. This should populate our cookie jar.
        """
        html = self.loadPage("https://www.linkedin.com/")
        soup = BeautifulSoup(html)
        csrf = soup.find(id="loginCsrfParam-login")['value']

        login_data = urllib.urlencode({
            'session_key': self.login,
            'session_password': self.password,
            'loginCsrfParam': csrf,
        })

        html = self.loadPage("https://www.linkedin.com/uas/login-submit", login_data)
        return

    def loadTitle(self):
        html = self.loadPage("http://www.linkedin.com/nhome")
        soup = BeautifulSoup(html)
        return soup.find("title")

    def logout(self):
        '''
        :return:
        '''
        os.remove(cookie_filename)

class LinkedinParserSelenium(object):
    '''
    '''
    def __init__(self,browser = 'Firefox',browser_loc='/home/madan/Downloads/phantomjs-2.1.1-linux-x86_64/bin/phantomjs',
                 visible = True,proxy = False,proxy_ip=None,proxy_port=None):
        '''
        :param browser:
        :param browser_loc:
        :param visible:
        :param proxy:
        :param proxy_ip: should be character??
        :param proxy_port: should be character??
        :return:
        '''
        if not visible:
            self.display = Display(visible=0, size=(800, 600))
            self.display.start()
        else:
            self.display = None
        if browser == 'PhantomJS':
            self.browser = webdriver.PhantomJS(browser_loc)
        else:
            firefox_profile = webdriver.FirefoxProfile()
            # firefox_profile.set_preference("browser.privatebrowsing.autostart", True)
            if proxy:
                if (not proxy_ip) or (not proxy_port) or (proxy_ip is None) or (proxy_port is None):
                    logging.error('No ip/port, not using proxy')
                else:
                    firefox_profile.set_preference("network.proxy.type", 1)
                    firefox_profile.set_preference("network.proxy.http", proxy_ip)
                    firefox_profile.set_preference("network.proxy.http_port", proxy_port)
                    firefox_profile.update_preferences()
            self.browser = webdriver.Firefox(firefox_profile=firefox_profile)
        self.browser.set_page_load_timeout(25)

    def login(self,username,password):
        '''
        :return:
        '''
        # self.browser = webdriver.PhantomJS()
        self.login(username,password)
        self.browser.get('https://www.linkedin.com/')
        username_field = self.browser.find_element_by_id("login-email")
        password_field = self.browser.find_element_by_id("login-password")
        username_field.send_keys(username)
        password_field.send_keys(password)
        self.browser.find_element_by_name("submit").click()

    def exit(self):
        '''
        :return:
        '''
        # self.browser.close()
        self.browser.quit()
        if self.display:
            self.display.stop()

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
