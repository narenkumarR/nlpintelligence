
import pandas as pd
from bs_crawl import BeautifulsoupCrawl as bsc

tmp = pd.read_excel('BuiltWith_Data_Classification.xlsx',sheetname=1)

# soup = bsc.single_wp('https://trends.builtwith.com/ads')
# soup = bsc.single_wp('https://trends.builtwith.com/ads/ad-network')
# chk = soup.find('div',{'class':'span8'}).find('tbody').findAll('tr')

def find_techs(url):
    '''
    :param tmp_in:
    :return:
    '''
    out_list = []
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
for ind in range(tmp.shape[0]):
    try:
        technology_class = tmp.iloc[ind,1].strip()
        technology_subclass = tmp.iloc[ind,3].strip()
    except:
        print 'error happened for ind : {}'.format(ind)
        continue
    link = tmp.iloc[ind,4]
    url = 'https://trends.builtwith.com'+link
    techs = find_techs(url)
    dic_key = (technology_class,technology_subclass)
    if dic_key in out_dic:
        out_dic[dic_key].extend(techs)
    else:
        out_dic[dic_key] = techs

# out_dic1 = {}
# for i in out_dic:
#     out_dic1[i] = []
#     for j in out_dic[i]:
#         if type(j) == list:
#             out_dic1[i].extend(j)
#         else:
#             out_dic1[i].append(j)

out_dic11 = {}
for i in out_dic:
    out_dic11[i] = list(set(out_dic[i]))

import pickle
with open('BW_technologies.pkl','w') as f:
    pickle.dump(out_dic11,f)

from url_cleaner import UrlCleaner
out_dic2 = {'Technology Class':[],'Technology SubClass':[], 'Technology':[], 'BW Trends URL':[]}
for key1,key2 in out_dic11:
    for name,url in out_dic11[(key1,key2)]:
        out_dic2['Technology Class'].append(key1)
        out_dic2['Technology SubClass'].append(key2)
        url = UrlCleaner().clean_url('http:'+url)
        out_dic2['BW Trends URL'].append(url)
        out_dic2['Technology'].append(name)

out_df = pd.DataFrame(out_dic2)
out_df.to_excel('BW_technologies.xls',index=False)

#looking at country urls
countries = ['United-States','United-Kingdom','Canada','Australia','France','Germany','Netherlands','Italy','Spain','Mexico','China','India','Japan']
# out_dic = {}
ind_list = range(tmp.shape[0])
import random
random.shuffle(ind_list)
for ind in ind_list:
    # print ind
    try:
        technology_class = tmp.iloc[ind,1].strip()
        technology_subclass = tmp.iloc[ind,3].strip()
    except:
        print 'error happened for ind : {}'.format(ind)
        continue
    link = tmp.iloc[ind,4]
    print technology_class,technology_subclass,link
    url = 'https://trends.builtwith.com'+link
    for country in countries:
        url = url+'/country/'+country
        techs = find_techs(url)
        dic_key = (technology_class,technology_subclass)
        if dic_key in out_dic:
            out_dic[dic_key].extend(techs)
        else:
            out_dic[dic_key] = techs

##after logging in. use selenium
from selenium_crawl import SeleniumParser
sp = SeleniumParser()
sp.browser.get('https://builtwith.com/login')
username = sp.browser.find_element_by_id("email")
password = sp.browser.find_element_by_id("password")
username.send_keys("ashwin@contractiq.com")
password.send_keys("ashwin0302")
sp.browser.find_element_by_name("ctl00$ctl00$content$contentAll$btnLogin").click()

def find_techs_selenium(url):
    '''
    :param url:
    :return:
    '''
    out_list = []
    soup = sp.get_soup(url)
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

for ind in range(tmp.shape[0]):
    try:
        technology_class = tmp.iloc[ind,1].strip()
        technology_subclass = tmp.iloc[ind,3].strip()
    except:
        print 'error happened for ind : {}'.format(ind)
        continue
    link = tmp.iloc[ind,4]
    url = 'https://trends.builtwith.com'+link
    techs = find_techs_selenium(url)
    dic_key = (technology_class,technology_subclass)
    if dic_key in out_dic:
        out_dic[dic_key].extend(techs)
    else:
        out_dic[dic_key] = techs


#report creation
sp.browser.get('https://trendspro.builtwith.com/createReport/builtwith')
from selenium.webdriver.support.ui import Select
select = Select(sp.browser.find_element_by_id('baseTech'))
select.select_by_value('addNew')
search_bar = sp.browser.find_element_by_id('techSearch')
search_bar.send_keys('PHP')
sp.browser.find_element_by_xpath("//body//div[@class='container']//div[@class='control-group']//button").click()

#reading matrix file
import pandas as pd
import re
tmp = pd.read_csv('data/016a20b3-3ba5-4012-9d71-be1fb3508759_matrix.csv',true_values='x',false_values='',nrows=10,skiprows=1)
from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:postgres@localhost:5432/builtwith_data')
cols = tmp.columns
cols = [re.sub(r'[^a-zA-Z0-9\.\-_]','',i.decode('ascii','ignore')) for i in cols]
tmp.columns = cols
max_col_size = 1600 #maximum 250-1600 columns per table. for safety assume 1000
for ind in range(tmp.shape[1]/max_col_size):
    tmp1 = tmp.iloc[:,ind*max_col_size:min(tmp.shape[1]+1,(ind+1)*max_col_size)]
    tmp1.to_sql('matrix_table_test_{}'.format(ind),engine,index=False,if_exists='replace')

tmp1 = pd.melt(tmp,id_vars=[tmp.columns[0]],value_vars=list(tmp.columns[1:]),var_name='Technology')
tmp2 = tmp1.loc[tmp1.value == True, ['Domain','Technology']]


import pandas as pd
from sqlalchemy import create_engine
tmp = pd.read_csv('data/nodejs_matrix.csv',true_values='x',false_values='',nrows=10,skiprows=1)
cols = tmp.columns
dtype_dict = {cols[0]:str}
for col in cols[1:]:
    dtype_dict[col] = bool

import pdb
import gc,re

def convert_to_long(csv_in,out_name,colnames,dtype_dict,single_set=1000,skiprows=2,to_db=False,db_engine=None,true_val='x'):
    if to_db:
        try:
            # start_row = skiprows
            # while True:
            #     print start_row
            #     # pdb.set_trace()
            #     tmp = pd.read_csv(csv_in,true_values='x',nrows=single_set,skiprows=start_row,dtype = dtype_dict,names=colnames)
            #     tmp1 = pd.melt(tmp,id_vars=[tmp.columns[0]],value_vars=list(tmp.columns[1:]),var_name='Technology')
            #     del tmp
            #     gc.collect()
            #     tmp2 = tmp1.loc[tmp1.value == True, ['Domain','Technology']]
            #     del tmp1
            #     gc.collect()
            #     tmp2.columns = ['company_url','technology']
            #     tmp2.to_sql(out_name,db_engine,index=False,if_exists='append')
            #     start_row += single_set
            #     del tmp2
            #     gc.collect()
            with open(csv_in,'r') as f:
                for _ in range(skiprows):
                    next(f)
                for line in f:
                    line = re.sub('\n','',line)
                    tmp = line.split(',')
                    comp_url = tmp[0]
                    inds =  [i for i, x in enumerate(tmp) if x == true_val]
                    cols_present = [colnames[i] for i in inds]

        except:
            print 'error happened'
        return
    else:
        try:
            # # pdb.set_trace()
            # with open(out_name,'w') as f:
            #     f.write("Domain,Technology\n")
            start_row = skiprows
            with open(out_name,'a') as f:
                while True:
                    print start_row
                    # pdb.set_trace()
                    tmp = pd.read_csv(csv_in,true_values='x',nrows=single_set,skiprows=start_row,dtype = dtype_dict,names=colnames)
                    tmp1 = pd.melt(tmp,id_vars=[tmp.columns[0]],value_vars=list(tmp.columns[1:]),var_name='Technology')
                    tmp2 = tmp1.loc[tmp1.value == True, ['Domain','Technology']]
                    tmp2.to_csv(f,header=False,index=False)
                    start_row += single_set
                    del tmp,tmp1,tmp2
                    gc.collect()
        except:
            pass
        return

# convert_to_long('data/nodejs_matrix.csv','data/long_test.csv',cols,dtype_dict,single_set=1000)

#creating table
'''
create table company_technology (
    company_url text,
    technology text
    --CONSTRAINT u_constraint UNIQUE (company_url, technology)
);
'''
engine = create_engine('postgresql://postgres:postgres@localhost:5432/builtwith_data')
convert_to_long('data/nodejs_matrix.csv','company_technology',cols,dtype_dict,single_set=1000,to_db=True,db_engine=engine)

# storing meta data
import os
import gc
import re
import bw_csv_reader
import pandas as pd
import logging
logging.basicConfig(filename='meta_dumping.log', level=logging.INFO)
from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:postgres@localhost:5432/builtwith_data')  
f_names = ['data/bw_meta_all/'+i for i in os.listdir('data/bw_meta_all/')]
# f_names = ['data/express_meta.csv', 'data/drupal_us_india_meta.csv', 'data/turn_meta.csv', 'data/wordpress_us_india_meta.csv', 'data/magento_meta.csv']
br = bw_csv_reader.BwCsvReader()
for f_name in f_names:
    print(f_name)
    print(re.sub('\.csv','',re.sub('data/','',f_name)))
    logging.info(f_name)
    # tmp = pd.read_csv(f_name)
    for tmp in br.read_csv(f_name):
        tmp['builtwith_source_technology'] = re.sub('\.csv','',re.sub('data/','',f_name))
        tmp.to_sql('companies_meta_data1_extra',engine,index=False,if_exists='append')
        del tmp
        gc.collect()

'''
require("RPostgreSQL")
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname = "builtwith_data",host = "localhost", port = 5432,user = "postgres", password = 'postgres')
f_names = c('data/heroku_meta.csv', 'data/wordpress_non_us_india_meta.csv', 'data/python_meta.csv')
for (f_name in f_names) {
    print(f_name)
    tmp = read.csv(f_name)
    tmp$builtwith_source_technology = gsub('data/','',gsub('.csv','',f_name))
    dbWriteTable(con, "companies_meta_data", value = tmp, append = TRUE, row.names = FALSE)
}
'''

##convert to long using pandas taking lot of time. try line by read. use threading
import itertools
import psycopg2
import re
from Queue import Queue
from threading import Thread

def matrix_csv_reader(csv_in,colnames,write_queue,skiprows=2,true_val='x'):
    '''
    :param csv_in:
    :param colnames:
    :param write_queue:
    :param skiprows:
    :param true_val:
    :return:
    '''
    with open(csv_in,'r') as f:
        for _ in range(skiprows):
            _ = next(f)
        ind = 0
        for line in f:
            ind += 1
            if ind%1000 == 0:
                print ind
            line = re.sub('\n','',line)
            tmp = line.split(',')
            comp_url = tmp[0]
            inds =  [i for i, x in enumerate(tmp) if x == true_val]
            cols_present = [colnames[i] for i in inds]
            iter_list = list(itertools.product([comp_url],cols_present))
            write_queue.put(iter_list)

def write_to_table(q,cursor,con,table_name):
    '''
    :param q:
    :param cursor:
    :param table_name:
    :return:
    '''
    # query = 'insert into {} values '.format(table_name)
    ind = 0
    while True:
        ind += 1
        iter_list = q.get()
        records_list_template = ','.join(['%s']*len(iter_list))
        insert_query = 'INSERT INTO company_technology VALUES {0}'.format(records_list_template)
        cursor.execute(insert_query, iter_list)
        if ind == 100:
            ind = 0
            con.commit()
        q.task_done()

def tech_matrix_to_table(csv_loc,skip_row=1):
    con = psycopg2.connect(database='builtwith_data', user='postgres',password='postgres',host='localhost')
    cursor = con.cursor()
    write_queue = Queue(maxsize=0)
    with open(csv_loc,'r') as f:
        for i in range(skip_row):
            _ = next(f)
        cols = next(f)
    cols = re.sub('\n','',cols)
    cols = cols.split(',')
    worker = Thread(target=write_to_table, args=(write_queue,cursor,con,"company_technology",))
    worker.daemon = True
    worker.start()
    matrix_csv_reader(csv_loc,cols,write_queue)
    write_queue.join()
    return True


# tech_matrix_to_table('data/nodejs_matrix.csv')
tech_matrix_to_table('data/magento_matrix.csv')
