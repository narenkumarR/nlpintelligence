__author__ = 'madan'

from bs_crawl import BeautifulsoupCrawl

def recursive_url(url,depth,final_depth):
    if depth == final_depth:
        return url
    else:
        soup = BeautifulsoupCrawl(url)
        newlink = soup.find('a') #find just the first one
        if len(newlink) == 0:
            return url
        else:
            return url, recursive_url(newlink,depth+1,final_depth)


soup = BeautifulsoupCrawl.single_wp('https://www.findyogi.com/')
for a in soup.find_all('a', href=True):
    print "Found the URL:", a['href'],'text:',a.text

#testing
import pandas as pd
import company_link_crawl
from bs_crawl import BeautifulsoupCrawl

company_crawler = company_link_crawl.CompanyPageCrawler()

companies = pd.read_csv('companies.csv')
res_list = []
for url in companies['Website']:
    print(url)
    try:
        contact_urls = company_crawler.get_all_contactlinks_urlinput(url,which_match=3)
        tmp = []
        for dic in contact_urls:
            tmp1 = [dic['url']]
            try:
                soup = BeautifulsoupCrawl.single_wp(dic['url'])
                phone_nos = company_crawler.get_phone_nos_soupinput(soup)
                emails = company_crawler.get_emails_in_page_soupinput(soup)
                tmp1.append(phone_nos)
                tmp1.append(emails)
            except:
                tmp1.extend([[],[]])
            tmp.append(tmp1)
        print(tmp)
        res_list.append((url,tmp))
    except:
        res_list.append((url,[]))
