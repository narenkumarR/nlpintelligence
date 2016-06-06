__author__ = 'joswin'

import psycopg2
con_bw = psycopg2.connect(database='builtwith_data', user='postgres',password='postgres',host='localhost')
cursor_bw = con_bw.cursor()

con_lkd = psycopg2.connect(database='linkedin_data', user='postgres',password='postgres',host='localhost')
cursor_lkd = con_lkd.cursor()


# cursor_lkd.execute('create table linkedin_company_urls_to_crawl_builtwith_extracted (url text UNIQUE)')
# con_lkd.commit()

query = ''' select "Domain","Company"  from companies_meta_data where not lower("LinkedIn") like '%linkedin%' and "Country" in ('IN','US') '''
from company_linkedin_url_extractor.company_extractor import CompanyLinkedinURLExtractorMulti
cc = CompanyLinkedinURLExtractorMulti()

offset = 100000
limit_no = 100

while True:
    print 'offset = {}'.format(offset)
    query_1 = query+' offset '+str(offset)+' limit '+str(limit_no)
    cursor_bw.execute(query_1)
    tmp = cursor_bw.fetchall()
    tmpdic = {}
    for i in tmp:
        if i[1]:
            tmpdic[i[1]] = i[0]
        else:
            tmpdic[i[0]] = i[0]
    out_dict = cc.get_linkedin_url_multi(tmpdic,n_threads=3,time_out=100)
    linkedin_urls = [(out_dict[i][0],) for i in out_dict]
    print('no of urls extracted:{}'.format(len(linkedin_urls)))
    records_list_template = ','.join(['%s']*len(linkedin_urls))
    insert_query = 'INSERT INTO {} VALUES {} ON CONFLICT DO NOTHING'.format('linkedin_company_urls_to_crawl_builtwith_extracted',records_list_template)
    cursor_lkd.execute(insert_query, linkedin_urls)
    con_lkd.commit()
    offset += 100



###using linkedin search from the other end
import psycopg2
con_bw = psycopg2.connect(database='builtwith_data', user='postgres',password='postgres',host='localhost')
cursor_bw = con_bw.cursor()

con_lkd = psycopg2.connect(database='linkedin_data', user='postgres',password='postgres',host='localhost')
cursor_lkd = con_lkd.cursor()

import re
import time
import urlparse
from linkedin_company_search.linkedin_company_search import LinkedinParser
from linkedin_company_search.proxy_generator import ProxyGen
pg = ProxyGen()

query = ''' select "Domain","Company"  from companies_meta_data where not lower("LinkedIn") like '%linkedin%' and "Country" in ('IN','US') '''

# offset = 789062
offset = 488550
limit_no = 50
proxy_list = pg.generate_proxy()
if proxy_list:
    proxy_ip,proxy_port = proxy_list.pop()

lp = LinkedinParser(proxy=True,proxy_ip=proxy_ip,proxy_port=proxy_port)
ind = 0
while True:
    try:
        ind += 1
        print 'offset = {}'.format(offset)
        query_1 = query+' offset '+str(offset)+' limit '+str(limit_no)
        cursor_bw.execute(query_1)
        tmp = cursor_bw.fetchall()
        name_list = [i[1].decode('ascii','ignore') for i in tmp if i[1]]
        # out_list = lp.search_company(name_list,initial_dic={'f_CCR':['us%3A0']},max_page=10)
        out_list = lp.search_company(name_list,initial_dic={},max_page=10)
        out_list_urls = [(urlparse.urljoin('https://www.linkedin.com',in_dic['url']),) for in_dic in out_list if 'url' in in_dic if re.search(r'company',in_dic['url'])]
        print len(out_list_urls)
        if len(out_list_urls)>0:
            records_list_template = ','.join(['%s']*len(out_list_urls))
            insert_query = 'INSERT INTO {} VALUES {} ON CONFLICT DO NOTHING'.format('linkedin_company_urls_to_crawl_builtwith_extracted',records_list_template)
            cursor_lkd.execute(insert_query, out_list_urls)
            con_lkd.commit()
            offset = offset - limit_no
        time.sleep(5)
        if ind%50 == 0:
            print('restarting the parser')
            lp.exit()
            if not proxy_list:
                print('no proxies available. get proxies')
                proxy_list = pg.generate_proxy()
                print('proxies fetched:{}'.format(proxy_list))
            try:
                proxy_ip,proxy_port = proxy_list.pop()
                print proxy_ip,proxy_port
                lp = LinkedinParser(proxy=True,proxy_ip=proxy_ip,proxy_port=proxy_port)
            except:
                print('error happened')
                lp = LinkedinParser()
            # lp = LinkedinParser()
    except:
        continue

