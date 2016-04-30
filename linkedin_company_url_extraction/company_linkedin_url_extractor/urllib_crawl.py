"""
File : urllib_crawl.py
Created On: 07-Mar-2016
Author: ideas2it
"""

import urllib2
import requests

class UrllibCrawl:

    @staticmethod
    def getResponse(baseurl,headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:39.0) Gecko/20100101 Firefox/39.0'}):
        # try:
        #     if headers:
        #         request_get = urllib2.Request(baseurl,data=None,\
        #                                   headers=headers)
        #     else:
        #         request_get = urllib2.Request(baseurl)
        #
        #     # define the request
        #     f_content=urllib2.urlopen(request_get)
        #
        #     # request, download and read the content
        #     response_content=f_content.read().decode('utf-8')
        #
        #     print(f_content.read().decode('utf-8'))
        #
        #     return response_content
        # except urllib2.URLError:
        #     request_get = requests.get(baseurl)
        #     return request_get.text
        try:
            request_get = requests.get(baseurl)
            return request_get.text
        except:
            return ''

    @staticmethod
    def getResponseProxy(baseurl):
        # print('getResponseProxy...baseurl..',baseurl)
        # '''request_get = urllib.request.Request(baseurl,data=None,headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:39.0) Gecko/20100101 Firefox/39.0'})
        #
        # proxies = {'http': 'http://64.62.233.67:80'}
        #
        # opener = urllib.request.FancyURLopener(proxies)
        #
        # f_content = opener.open(baseurl)
        # print('f_content..',f_content.read())
        # # request, download and read the content
        # response_content=f_content.read().decode('utf-8')'''
        # print('response_content.proxy....',baseurl)
        # proxy = urllib.request.ProxyHandler({'http': 'http://108.24.48.71:80'})
        # auth = urllib.request.HTTPBasicAuthHandler()
        # opener = urllib.request.build_opener(proxy, auth, urllib.request.HTTPHandler)
        # urllib.request.install_opener(opener)
        # conn = urllib.request.urlopen(baseurl)
        # response_content = conn.read()
        #
        # return response_content
        pass