__author__ = 'joswin'

from extract_info import WebPageExtractor

wpe = WebPageExtractor()
res = wpe.search_webpage_base('http://www.madkudu.com/',['monthly subscription','per month','/month'],[0.9,0.8,0.7])



# fetching from log files
with open('website_extraction_1.log','r') as f:
    tmp = f.readlines()

tmp1 = [i for i in tmp if 'Extracted info: urls:' in i or 'Trying for url :' in i]

out_dic = {'website':[],'score':[],'emails':[],'urls':[],'match_texts_test':[]}
ind = 0
while ind<len(tmp1):
    inp = tmp1[ind][:-1]
    if not 'Trying for url :' in inp:
        ind += 1
        continue
    website = inp.split('Trying for url :')[1][:-1].strip()
    ind += 1
    inp = tmp1[ind][:-1]
    if not 'Extracted info:' in inp:
        continue
    vals = inp.split('Extracted info: ')[1].split(': ')
    urls = eval(inp.split(': urls: ')[1].split(', emails:')[0])
    mails = eval(inp.split(': urls: ')[1].split(', emails:')[1].split(',matches :')[0])
    matches = eval(inp.split(': urls: ')[1].split(', emails:')[1].split(',matches :')[1].split(',weight:')[0])
    weight = eval(inp.split(': urls: ')[1].split(', emails:')[1].split(',matches :')[1].split(',weight:')[1])
    out_dic['website'].append(website)
    out_dic['score'].append(weight)
    out_dic['emails'].append(mails)
    out_dic['urls'].append(urls)
    out_dic['match_texts_test'].append(matches)
    ind += 1
