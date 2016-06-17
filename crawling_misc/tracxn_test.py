__author__ = 'joswin'
import tracxn_crawling
from selenium.webdriver.common.keys import Keys

years = ['2015','2014','2012%2C2011','2013%2C2010','2016%2C2008%2C2007%2C2006%2C2009']
practise_areas = ['Geo - India Tech','Enterprise Applications','AdTech','Enterprise Infrastructure','Consumer','Mobile','Geo - India Offline','FinTech','HealthTech','EdTech','Technology','Energy','AgriTech','Geo - South East Asia','Geo - China']

states = ['Karnataka', 'Maharashtra', 'NCT', 'Tamil Nadu', 'Andhra Pradesh', 'Haryana', 'Gujarat', 'Uttar Pradesh', 'Kerala', 'Bengal', 'Rajasthan', 'Madhya Pradesh', 'Punjab', 'Goa', 'Odisha', 'Union Territory Of Chandigarh', 'Union Territory of Chandigarh', 'Jharkhand', 'Chhattisgarh', 'Uttarakhand', 'Assam']
#login after this

# url = 'http://tracxn.com/query/feedview#|query%3Dsaas|sort%3Drelevance|order%3DDEFAULT|country%3DIndia|state%3DUttar%20Pradesh|practiceArea%3DGeo%20-%20India%20Tech'

states_major = ['Karnataka', 'Maharashtra', 'NCT', 'Tamil%20Nadu', 'Andhra%20Pradesh','Haryana','Gujarat%2CUttar%20Pradesh',
                'Kerala%2CPunjab%2CGoa%2COdisha%2CUnion%20Territory%20Of%20Chandigarh%2CUnion%20Territory%20of%20Chandigarh%2CJharkhand%2CChhattisgarh%2CUttarakhand%2CAssam%2CHimachal%20Pradesh%2CKashmir%2CUnion%20Territory%20Of%20Puducherry%2CUnion%20Territory%20of%20Puducherry',
                'Madhya Pradesh%2CRajasthan%2CBengal']
practise_major =  ['Geo - India Tech','Enterprise Applications',
                   'AdTech%2CGeo - India Offline%2CFinTech','Consumer%2CEnterprise%20Infrastructure%2CMobile',
                   'AgriTech%2CGeo - South East Asia%2CHealthTech%2CEdTech%2CEnergy%2CTechnology']
out_list = []
import time
from random import randint
ind = 0
prev_len = 0
ts = tracxn_crawling.TracxnScrapper()
for year in years:
    for practise in practise_major:
        for state in states_major:
            print ind
            if ind < 14:
                ind += 1
                continue
            ts.sp.browser.find_element_by_tag_name('body').send_keys(Keys.CONTROL + 't')
            ts.sp.browser.find_element_by_tag_name('body').send_keys(Keys.CONTROL + Keys.TAB)
            ts.sp.browser.find_element_by_tag_name('body').send_keys(Keys.CONTROL + 'w')
            url = "http://tracxn.com/query/feedview#|query%3Dsaas|"\
            "sort%3Drelevance|order%3DDEFAULT|country%3DIndia|state%3D{}|isFunded%3Dfalse|"\
            "practiceArea%3D{}|foundedYear%3D{}".format(state,'%20'.join(practise.split(' ')),year)
            ts.sp.browser.get(url)
            time.sleep(10)
            out_list.extend(ts.page_scrapper())
            print(len(out_list))
            if len(out_list) == prev_len:
                time.sleep(10)
                out_list.extend(ts.page_scrapper())
            if len(out_list) == prev_len:
                time.sleep(10)
                out_list.extend(ts.page_scrapper())
            if len(out_list) == prev_len:
                print year,practise,state
            time.sleep(randint(2,5))
            ind += 1
            prev_len = len(out_list)
            print out_list[-1]

import pickle
with open('tracxn_list3.pkl','w') as f:
    pickle.dump(out_list,f)

ind = 0
prev_len = 1860
states_minor = list(set(states)-set(states_major))
for year in years:
    for state in states_minor:
        print ind
        if ind < 0:
            ind += 1
            continue
        ts.sp.browser.find_element_by_tag_name('body').send_keys(Keys.CONTROL + 't')
        url = "http://tracxn.com/query/feedview#|query%3Dsaas|"\
        "sort%3Drelevance|order%3DDEFAULT|country%3DIndia|state%3D{}|"\
        "foundedYear%3D{}".format('%20'.join(state.split(' ')),year)
        ts.sp.browser.get(url)
        time.sleep(20)
        out_list.extend(ts.page_scrapper())
        print(len(out_list))
        if len(out_list) == prev_len:
            time.sleep(20)
            out_list.extend(ts.page_scrapper())
        if len(out_list) == prev_len:
            time.sleep(20)
            out_list.extend(ts.page_scrapper())
        if len(out_list) == prev_len:
            print year,state
        time.sleep(randint(2,5))
        ind += 1
        prev_len = len(out_list)
        ts.sp.browser.find_element_by_tag_name('body').send_keys(Keys.CONTROL + 'w')


###reading
with open('tracxn_list3.pkl','r') as f:
    tlist = pickle.load(f)

tdic={}
for i in range(len(tlist)):
    tdic[i] = tlist[i]

import pandas as pd
tdf = pd.DataFrame.from_dict(tdic,'index')
tdf_unique = tdf.drop_duplicates(['name'])



