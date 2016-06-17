__author__ = 'madan'
from selenium_crawl import SeleniumParser
from bs4 import BeautifulSoup
import re

sp = SeleniumParser()
soup = sp.get_soup('http://www.zigwheels.com/dealers')
divs = soup.find_all('div',{'class':'row fxHeight'})
links = [j.find('div',{'class':'dealersTitle clearfix'}).find('div',{'class':'dlText fl'}).find('a')['href'][:-1] for i in divs for j in i.find_all('div',{'class':'col-lg-3 col-md-3 col-sm-6 MT10 MB10'})]
links_dic = {}
for link in links:
    tmp = sp.get_soup(link)
    dets = [(j.find('a').text,j.find('a')['href'])  for i in tmp.find_all('div',{'class':'col-lg-3 col-md-3 col-sm-3 makeLinkContent PL20'}) for j in i.find_all('li',{'class':'MT5 MB5'})]
    dets = [i for i in dets if not re.search('dealers',i[0],re.IGNORECASE)]
    links_dic[re.split('/',link)[-1]] = dets

'''
tmp1 = tmp.find_all('div',{'class':'col-lg-12 col-md-12 col-sm-12 col-xs-12 brdBT brdTP brdLT brdRT MT25 PZ'})

name tmp1[0].find('span',{'class':'fnt_22B'}).text
address re.sub('Address:-','',tmp1[0].find_all('span')[1].text)
ph nos : re.split(',',re.sub('Phone : ','',tmp1[0].find('span',{'class':'hidden-xs'}).text))
email re.sub('Email|:|-','',tmp1[0].find('span',{'style':'display:block'}).text).strip()


'''
import time,pickle
out_dic = {'Brand':[],'Place':[],'Name':[],'Address':[],'Phone':[],'Email':[]}
for i in links_dic.keys()[9:]:
    for place,link in links_dic[i]:
        try:
            name,address,ph,email = '','','',''
            tmp = sp.get_soup(link)
            tmp1 = tmp.find_all('div',{'class':'col-lg-12 col-md-12 col-sm-12 col-xs-12 brdBT brdTP brdLT brdRT MT25 PZ'})
            for j in tmp1:
                try:
                    name = j.find('span',{'class':'fnt_22B'}).text.strip()
                except:
                    continue
                try:
                    address = re.sub('Address:-','',j.find_all('span')[1].text)
                except:
                    pass
                try:
                    ph = re.sub('Phone : ','',j.find('span',{'class':'hidden-xs'}).text)
                except:
                    pass
                try:
                    email = re.sub('Email|:|-','',j.find('span',{'style':'display:block'}).text).strip()
                except:
                    pass
                out_dic['Brand'].append(i)
                out_dic['Place'].append(place)
                out_dic['Name'].append(name)
                out_dic['Address'].append(address)
                out_dic['Phone'].append(ph)
                out_dic['Email'].append(email)
                time.sleep(2)
        except:
            print(i,place,link)

with open('out_dic.pkl','w') as f:
    pickle.dump(out_dic,f)
