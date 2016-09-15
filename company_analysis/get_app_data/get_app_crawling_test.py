__author__ = 'joswin'
import selenium_crawl
from urlparse import urljoin
import time
from random import randint
link_parser = selenium_crawl.SeleniumParser('firefox',visible=True)
soup = link_parser.get_soup('https://www.getapp.com/browse')
blocks = soup.find_all('div',{'class':'block'})
urls = []
for block in blocks:
    block_as = block.find_all('a')
    for a in block_as:
        url = urljoin('https://www.getapp.com/',a['href'])
        urls.append(url)

base_urls = [url for url in urls if len(url.split('/'))==5]
rest_urls = list(set(urls)-set(base_urls))
out_dic = {}
company_urls = []
for url in base_urls:
    print(url)
    try:
        time.sleep(randint(2,5))
        soup1 = link_parser.get_soup(url)
        list_items = soup1.find('ul',{'class':'serp list-unstyled'}).find_all('li',{'class':'row listing_entry'})
        next_pages = soup1.find('ul',{'class':'pagination pagination-lg'}).find_all('li')
        total_pages = int(next_pages[-2].text)
        next_urls = ['{}?page={}'.format(url,i) for i in range(total_pages+1)[2:]]
        rest_urls.extend(next_urls)
        comp_urls = [urljoin('https://www.getapp.com/',ii.find('h2').find('a')['href']) for ii in list_items]
        company_urls.extend(comp_urls)
    except:
        continue

company_urls = list(set(company_urls))
for url in rest_urls:
    print(url)
    try:
        time.sleep(randint(2,5))
        soup1 = link_parser.get_soup(url)
        list_items = soup1.find('ul',{'class':'serp list-unstyled'}).find_all('li',{'class':'row listing_entry'})
        next_pages = soup1.find('ul',{'class':'pagination pagination-lg'}).find_all('li')
        total_pages = int(next_pages[-2].text)
        for i in range(total_pages+1)[2:]:
            next_url = '{}?page={}'.format(url,i)
            if next_url not in rest_urls:
                rest_urls.append(next_url)
        comp_urls = [urljoin('https://www.getapp.com/',ii.find('h2').find('a')['href']) for ii in list_items]
        company_urls.extend(comp_urls)
    except:
        continue



from get_app_data.get_app_crawling import GetAppCompanyPage
gacp = GetAppCompanyPage()
from proxy_generator import ProxyGen
proxy_generator = ProxyGen(visible=False,page_load_timeout=60)
proxies = proxy_generator.generate_proxy()
ip,port = proxies.pop()
proxy_generator.exit()

det_dic = {}
import pickle
link_parser = selenium_crawl.SeleniumParser('firefox',visible=True,proxy=True,proxy_ip=ip,proxy_port=port,page_load_timeout=100)
for url in company_urls:
    url = url.split('?')[0]
    url = ''.join(url.split('/x'))
    print url
    soup2 = link_parser.get_soup(url)
    det_dic[url] = gacp.get_details(soup2)
    with open('get_app_company.pkl','w') as f:
        pickle.dump(det_dic,f)
    time.sleep(randint(2,5))

for url in rest_urls:
    try:
        soup2 = link_parser.get_soup(url)
        det_dic[url] = gacp.get_details(soup2)
        with open('get_app_company.pkl','w') as f:
            pickle.dump(det_dic,f)
        time.sleep(randint(2,5))
    except:
        continue

# shuffle urls, used 4 as seed