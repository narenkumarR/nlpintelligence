__author__ = 'joswin'
import re
from bs4 import BeautifulSoup
from selenium_crawl import SeleniumParser


#log in to the page
#build search page

class TracxnScrapper(object):
    def __init__(self):
        self.sp = SeleniumParser()
        self.sp.browser.get('http://tracxn.com/')

    def page_scrapper(self):
        ''' search page result scrapper
        :return:
        '''
        #scroll from top
        self.sp.browser.execute_script("window.scrollTo(0, 0)")
        html = self.sp.browser.page_source
        soup0 = BeautifulSoup(html)
        #scrol in middle
        self.sp.browser.execute_script("window.scrollTo(0, 2500)")
        html = self.sp.browser.page_source
        soup1 = BeautifulSoup(html)
        #scrol in bottom
        self.sp.browser.execute_script("window.scrollTo(0, 5000)")
        html = self.sp.browser.page_source
        soup2 = BeautifulSoup(html)
        #extracting details
        try:
            tmp0 = soup0.find('div',{'class':'header-space-translate'}).find('div').find('div').find_all('div',{'class':'row clearfix'})
            tmp00 = soup0.find('div',{'class':'header-space-translate'}).find('div').find('div').find_all('div',{'class':'row clearfix col-xs-12 column col-no-pad'})
        except:
            tmp0,tmp00 = [],[]
        try:
            tmp1 = soup1.find('div',{'class':'header-space-translate'}).find('div').find('div').find_all('div',{'class':'row clearfix'})
            tmp11 = soup1.find('div',{'class':'header-space-translate'}).find('div').find('div').find_all('div',{'class':'row clearfix col-xs-12 column col-no-pad'})
        except:
            tmp1,tmp11 = [],[]
        try:
            tmp2 = soup2.find('div',{'class':'header-space-translate'}).find('div').find('div').find_all('div',{'class':'row clearfix'})
            tmp22 = soup2.find('div',{'class':'header-space-translate'}).find('div').find('div').find_all('div',{'class':'row clearfix col-xs-12 column col-no-pad'})
        except:
            tmp2,tmp22 = [],[]
        #merging all and removing duplicates
        tmp_list = [(tmp0[i],tmp00[i]) for i in range(min(len(tmp0),len(tmp00)))]
        tmp_list.extend([(tmp1[i],tmp11[i]) for i in range(min(len(tmp1),len(tmp11)))])
        tmp_list.extend([(tmp2[i],tmp22[i]) for i in range(min(len(tmp2),len(tmp22)))])
        tmp_list = list(set(tmp_list))
        #fetching details
        det_list = []
        for tmp0,tmp00 in tmp_list:
            det = {}
            try:
                det['name'] = tmp0.find('div',{'class':'fc-mobile-content'}).find('span').text
                det['description'] = tmp0.find('div',{'class':'fc-mobile-content'}).find('div',{'class':'company-short-description'}).text
            except:
                continue
            for chk in tmp0.find('span',{'class':'dot-separate-row'}).find_all('span',{'class':'dot-separate'}):
                try:
                    chk1 = chk.find('span')
                    det[chk1.get('class')[2]] = chk1.text
                except:
                    pass
            try:
                det['Feed details'] = tmp0.find('div',{'class':'business-modal-also-in'}).find_all('div',{'class':'pad-also-in'})[0].find_all('span',{'title':re.compile('^Feed Name')})[0].text
                det['Business Model'] = tmp0.find('div',{'class':'business-modal-also-in'}).find_all('div',{'class':'pad-also-in'})[0].find_all('span',{'title':re.compile('^Business Model')})[0].text
            except:
                pass
            people_details = {}
            for chk in tmp00.find_all('div',{'id':re.compile('[0-9]')})[:-1]:
                try:
                    people_details[chk.find('span').find('span').get('title')] = (re.sub('\\xa0','',chk.find('span').find('span').text),\
                                                                                chk.find('span').find('span').find('a')['href'],chk.text)
                except:
                    pass
            det['People details'] = people_details
            try:
                det['company_text'] = tmp00.find_all('div',{'id':re.compile('[0-9]')})[-1].text
            except:
                pass
            det_list.append(det)
        return det_list

