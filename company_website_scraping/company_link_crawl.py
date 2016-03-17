__author__ = 'madan'

import re
import pdb
from urlparse import urljoin

from bs_crawl import BeautifulsoupCrawl
import url_cleaner

class CompanyLinkCrawler(object):
    def __init__(self):
        self.url_cleaner = url_cleaner.UrlCleaner()
        self.soup_generator = BeautifulsoupCrawl

    def get_pagetext_soupinput(self,soup):
        '''
        :param soup:
        :return:
        '''
        #following removes too much.. sometimes no text remains
        # [s.extract() for s in soup(['style', 'script', '[document]', 'head', 'title'])]
        # return soup.getText()
        def visible(element):
            if element.parent.name in ['style', 'script', '[document]', 'head', 'title']:
                return False
            elif re.match('<!--.*-->', str(element.encode('ascii','ignore'))):
                return False
            return True
        texts = soup.findAll(text=True)
        visible_texts = filter(visible, texts)
        return '\n'.join(visible_texts)

    def get_pagetext_urlinput(self,url):
        '''
        :param url:
        :return:
        '''
        soup = self.soup_generator.single_wp(url)
        return self.get_pagetext_soupinput(soup)

    def get_all_links_urlinput(self,url):
        ''' Get all the links in a company page
        :param url:
        :return:
        '''
        soup = self.soup_generator.single_wp(url)
        res_list = self.get_all_links_soupinput(soup,url)
        return res_list

    def get_all_links_soupinput(self,soup,base_url):
        ''' Get all links from a soup input
        :param soup:
        :return:
        '''
        # pdb.set_trace()
        res_list = []
        for a in soup.find_all('a', href=True):
            url = a['href']
            if '@' in url:
                continue
            url = urljoin(base_url,url)
            url = self.url_cleaner.clean_url(url)
            res_list.append({"url":url,'url_text':a.text})
        return res_list

    def check_link_text(self,url,url_text,url_match_string,url_text_match_string):
        ''' check if a url and text have a match
        :param url:
        :param url_text:
        :param url_match_string:
        :param url_text_match_string:
        :return:
        '''
        url_matching, text_matching,both_matching,any_matching = False,False,False,False
        if re.search(url_match_string,url,re.IGNORECASE):
            url_matching = True
        if re.search(url_text_match_string,url_text,re.IGNORECASE):
            text_matching = True
        if url_matching and text_matching:
            both_matching = True
        if url_matching or text_matching:
            any_matching = True
        return url_matching,text_matching,both_matching,any_matching

    def search_links_textmatch(self,linktext,url_match_string,url_text_match_string,which_match = 2):
        '''
        :param linktext: list of dictionaries from get_all_links_soupinput function.
        :param url_match_string: search for match in url
        :param url_text_match_string: search for match in text
        :param which_match : 0 for url match, 1 for text match, 2 for both match
        :return:list of links which have info about company like contact us/about us etc
        '''
        out_list = []
        for dic in linktext:
            link,text = dic['url'],dic['url_text']
            if self.check_link_text(link,text,url_match_string,url_text_match_string)[which_match]:
                out_list.append(dic)
        return out_list

    def get_all_contactlinks_soupinput(self,soup,base_url,url_match_string=None,url_text_match_string=None,which_match=3):
        '''
        :param soup:
        :return:
        '''
        if not url_match_string:
            url_match_string = 'about|contact|team'
        if not url_text_match_string:
            url_text_match_string = 'about|contact|team'
        linktexts = self.get_all_links_soupinput(soup,base_url)
        contact_linktexts = self.search_links_textmatch(linktexts,url_match_string,url_text_match_string,which_match=which_match)
        return contact_linktexts

    def get_all_contactlinks_urlinput(self,url,url_match_string=None,url_text_match_string=None,which_match=3):
        '''
        :param url:
        :param url_match_string:
        :param url_text_match_string:
        :return:
        '''
        soup = self.soup_generator.single_wp(url)
        return self.get_all_contactlinks_soupinput(soup,url,url_match_string,url_text_match_string,which_match)

    def get_social_links_soupinput(self,soup,base_url,match_list=None):
        '''Fetch links like facebook, linkedin etc
        :param soup:
        :param match_list:
        :return:
        '''
        # pdb.set_trace()
        if not match_list:
            match_list = ['facebook','twitter','linkedin','crunchbase','angel.co','plus.google']
        match_string = '('+')|('.join(match_list)+')'
        linktexts = self.get_all_links_soupinput(soup,base_url)
        return self.search_links_textmatch(linktexts,match_string,'fdajsfsgadhhsdkeaw',which_match=0)

    def get_social_links_urlinput(self,url,match_list=None):
        '''
        :param url:
        :param match_list:
        :return:
        '''
        soup = self.soup_generator.single_wp(url)
        return self.get_social_links_soupinput(soup,match_list,url)

    def get_emails_in_page_soupinput(self,soup):
        '''
        :param soup:
        :return:
        '''
        ph_reg = r'[\w\.-]+@[\w\.-]+'
        emails = []
        page_text = soup.text
        for match in re.finditer(ph_reg,page_text):
            match_text = page_text[match.start():match.end()]
            if '.' in match_text[match_text.find('@'):] \
                    and len(match_text[match_text.rfind('.'):])<6:
                emails.append(match_text)
        page_text = self.get_pagetext_soupinput(soup)
        for match in re.finditer(ph_reg,page_text):
            match_text = page_text[match.start():match.end()]
            if '.' in match_text[match_text.find('@'):]\
                    and len(match_text[match_text.rfind('.'):])<6:
                emails.append(match_text)
        return list(set(emails))

    def get_phone_nos_soupinput(self,soup):
        '''
        :param soup:
        :return:
        '''
        ph_reg = r'([\+][0-9]{1,3}([ \.\-])?)?([\(]{1}[0-9]{3}[\)])?([0-9A-Z \.\-]{1,32})((x|ext|extension)?[0-9]{1,4}?)'
        page_text = self.get_pagetext_soupinput(soup)
        matches = []
        for match in re.finditer(ph_reg,page_text):
            match_text = page_text[match.start():match.end()]
            match_text = re.sub(r'[ -]','',match_text)
            match_text = re.sub(r'[a-zA-Z]','',match_text)
            if len(match_text)>=10:
                matches.append(match_text)
        return list(set(matches))

class CompanyMainLinkCrawler(object):
    ''' wrapper fro CompanyLinkCrawler
    '''
    def __init__(self):
        self.company_link_crawler = CompanyLinkCrawler()

    def get_contact_details_urlinput(self,url):
        ''' get all emails and phone nos
        :param url:
        :return:
        '''
        soup = self.company_link_crawler.soup_generator.single_wp(url)
        emails, phones, urls = [],[],[]
        emails.extend(self.company_link_crawler.get_emails_in_page_soupinput(soup))
        phones.extend(self.company_link_crawler.get_phone_nos_soupinput(soup))
        linktexts = self.company_link_crawler.get_all_contactlinks_soupinput(soup,url)
        for dic in linktexts:
            url1 = dic['url']
            urls.append(url1)
            try:
                soup = self.company_link_crawler.soup_generator.single_wp(url1)
                emails.extend(self.company_link_crawler.get_emails_in_page_soupinput(soup))
                phones.extend(self.company_link_crawler.get_phone_nos_soupinput(soup))
            except:
                continue
        return list(set(urls)),list(set(emails)),list(set(phones))