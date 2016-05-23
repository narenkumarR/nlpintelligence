
import pandas as pd
from bs_crawl import BeautifulsoupCrawl as bsc

tmp = pd.read_excel('BuiltWith_Data_Classification.xlsx',sheetname=1)

# soup = bsc.single_wp('https://trends.builtwith.com/ads')
# soup = bsc.single_wp('https://trends.builtwith.com/ads/ad-network')
# chk = soup.find('div',{'class':'span8'}).find('tbody').findAll('tr')

def find_techs(tmp_in):
    '''
    :param tmp_in:
    :return:
    '''
    out_list = []
    url = 'https://trends.builtwith.com'+tmp_in
    soup = bsc.single_wp(url)
    try:
        trs = soup.find('div',{'class':'span8'}).find('tbody').findAll('tr')
        for tr in trs:
            try:
                text = tr.find('a').text
                url = tr.find('a')['href']
                out_list.append((text,url))
            except:
                continue
    except:
        pass
    return out_list

out_dic = {}
for tmp_in in list(tmp.iloc[:,1])[154:]:
    print(tmp_in)
    if tmp_in.count('/') == 1:
        base_tech = True
    else:
        base_tech = False
    techs = find_techs(tmp_in)
    print techs
    if base_tech:
        dic_key = tmp_in[1:]
    else:
        dic_key = tmp_in[1:tmp_in[1:].find('/')+1]
    if dic_key in out_dic:
        out_dic[dic_key].append(techs)
    else:
        out_dic[dic_key] = techs

out_dic1 = {}
for i in out_dic:
    out_dic1[i] = []
    for j in out_dic[i]:
        if type(j) == list:
            out_dic1[i].extend(j)
        else:
            out_dic1[i].append(j)

out_dic11 = {}
for i in out_dic1:
    out_dic11[i] = list(set(out_dic1[i]))

import pickle
with open('BW_technologies.pkl','w') as f:
    pickle.dump(out_dic11,f)

from url_cleaner import UrlCleaner
out_dic2 = {'Base Group':[],'Technology':[],'URL':[]}
for i in out_dic11:
    for j,k in out_dic11[i]:
        out_dic2['Base Group'].append(i)
        out_dic2['Technology'].append(j)
        k = UrlCleaner().clean_url('http:'+k)
        out_dic2['URL'].append(k)

out_df = pd.DataFrame(out_dic2)