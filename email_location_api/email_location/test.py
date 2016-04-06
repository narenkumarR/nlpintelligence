import pandas as pd
import re
import email_location_finder
import time
import urllib2
mails = pd.read_excel("/home/madan/Desktop/joswin_bck/toPendrive/works/pipecandy/shrikanth_docs/campaign_replies_preprocessed.xls")
emails = list(set(mails['Email']))
emails = [mail[re.search(r'[\w\.-]+@[\w\.-]+',mail).start():re.search(r'[\w\.-]+@[\w\.-]+',mail).end()] for mail in emails if re.search(r'[\w\.-]+@[\w\.-]+',mail)]
emails = list(set(emails))
locations = []
other_details = []
for mailid in emails:
    try:
        location,otherdetails = email_location_finder.EmailLocationFinder().get_email_loc_linkedin_duckduckgo(mailid)
        print mailid,location,otherdetails
        locations.append(location)
        other_details.append(str(otherdetails))
    except urllib2.HTTPError as e:
        print mailid,'httperror',e,'wait for 10 seconds'
        locations.append('got http error')
        other_details.append('')
        time.sleep(10)
    except Exception as e:
        print mailid,e
        locations.append('got error')
        other_details.append('')

tmp = pd.DataFrame({'Email':emails})
tmp['Location'] = locations
urls, res_froms = [], []
for i in other_details:
    try:
        ii = eval(i)
        urls.append(ii['url'])
        res_froms.append(ii['res_from'])
    except :
        urls.append('')
        res_froms.append('')

tmp['URL'] = urls
tmp['Result From'] = res_froms
tmp['Details'] = other_details
tmp.to_excel('email_locations_onlyDDG.xls')

#names given by ashwin
import time,datetime
import pandas as pd
import urllib2
import pdb,logging
import email_location_finder
logging.basicConfig(filename='log_file.log',level=logging.DEBUG)
data = pd.read_excel("Angel list - cleaned up list.xls")
names_list,companies_list,locations, other_details, urls, res_froms = [],[],[],[],[],[]
ind = 2378
#from 375 new algo
#from 563 better algo
while ind < data.shape[0]:
    print(ind)
    if ind % 20 == 0:
        print '10 second wait'
        time.sleep(10)
    company,names = data.iloc[ind,[2,4]]
    if pd.isnull(company):
        company = ''
    if pd.isnull(names):
        names = ''
    names = names.strip().split(';')
    ind1 = 0
    while ind1 < len(names):
        name = names[ind1]
        if name and type(name) in [unicode,str] and type(company) in [unicode,str]:
            try:
                # query = "site:linkedin.com "+name+' '+company
                # location,otherdetails = email_location_finder.EmailLocationFinder().ddg_linkedin_name_company_match('','',query,True)
                location,otherdetails = email_location_finder.EmailLocationFinder().ddg_linkedin_name_company_match(name,company)
                url, res_from = otherdetails['url'],otherdetails['res_from']
                logging.info(str(datetime.datetime.now())+':'+name+', '+company+', '+location+', '+str(otherdetails)+'\n')
            except urllib2.HTTPError as e:
                logging.info(str(datetime.datetime.now())+':'+'httperror'+str(e)+' wait for 40 seconds: '+name+', '+company+'\n')
                print 'httperror '+str(e)+' 40 second wait'
                time.sleep(40)
                # continue
            except Exception as e:
                # pdb.set_trace()
                location,otherdetails, url, res_from = '',{},'',''
                logging.info(str(datetime.datetime.now())+':'+'Error/No results : '+str(e)+','+name+', '+company+'\n')
            names_list.append(name)
            companies_list.append(company)
            locations.append(location)
            other_details.append(otherdetails)
            urls.append(url)
            res_froms.append(res_from)
        ind1 += 1
    ind += 1

tmp = pd.DataFrame({'Name':names_list})
tmp['Company'] = companies_list
tmp['Location'] = locations
tmp['LinkedIn url'] = urls
tmp['res_from'] = res_froms
tmp['other details'] = other_details
tmp.to_excel('tmp_locs.xls',index=False)


##finding match
import nltk
tmp = pd.read_excel('all_names_locs.xls')
tmp1 = pd.read_excel('names_locations_linkedin.xls')
res = pd.merge(tmp1,tmp,on=['Name','Company'])
res.to_excel('linkedin_results1.xls')

matchflag = []
for ind in range(res.shape[0]):
    link_locs, angel_locs = [],[]
    if res.loc[ind,'Location_x'] and not pd.isnull(res.loc[ind,'Location_x']):
        link_locs=nltk.word_tokenize(re.sub(';',' ',res.loc[ind,'Location_x']).lower())
        link_locs = [i for i in link_locs if len(i)>2]
    if res.loc[ind,'Location_y'] and not pd.isnull(res.loc[ind,'Location_y']):
        angel_locs=nltk.word_tokenize(re.sub(';',' ',res.loc[ind,'Location_y']).lower())    
        angel_locs = [i for i in angel_locs if len(i)>2]
    if not link_locs:
        matchflag.append('No results fetched')
    elif not angel_locs:
        matchflag.append('No location in angellist')
    else:
        if len(set(link_locs)&set(angel_locs))>0:
            matchflag.append('Matching result')
        else:
            matchflag.append('Not matching result')
