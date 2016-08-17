__author__ = 'joswin'
import quotequail
def quotequail_mail_split(sent):
    tmp=quotequail.quote(sent)
    quoted_part, unquoted_part = '',''
    for flag,text in tmp:
        if len(text)>0:
            if flag:
                unquoted_part += text
            else:
                quoted_part += text
    return unquoted_part,quoted_part

prev_mail_start = ['On Sun[a-z]*[ ,]+[\w0-9]+','On Mon[a-z]*[ ,]+[\w0-9]+','On Tue[a-z]*[ ,]+[\w0-9]+'
                    ,'On Wed[a-z]*[ ,]+[\w0-9]+','On Thu[a-z]*[ ,]+[\w0-9]+','On Fri[a-z]*[ ,]+[\w0-9]+'
                    ,'On Sat[a-z]*[ ,]+[\w0-9]+'\
                   ,'On Jan[a-z]*[ -/][0-9]+','On Feb[a-z]*[ -/][0-9]+','On Mar[a-z]*[ -/][0-9]+','On Apr[a-z]*[ -/][0-9]+'
                   ,'On May[a-z]*[ -/][0-9]+','On Jun[a-z]*[ -/][0-9]+'\
                    ,'On Jul[a-z]*[ -/][0-9]+','On Aug[a-z]*[ -/][0-9]+','On Sep[a-z]*[ -/][0-9]+','On Oct[a-z]*[ -/][0-9]+'
                    ,'On Nov[a-z]*[ -/][0-9]+','On Dec[a-z]*[ -/][0-9]+'
                   ,'On [0-9]+[ -/]Jan[a-z]*','On [0-9]+[ -/]Feb[a-z]*','On [0-9]+[ -/]Mar[a-z]*','On [0-9]+[ -/]Apr[a-z]*'
                   ,'On [0-9]+[ -/]May[a-z]*','On [0-9]+[ -/]Jun[a-z]*'\
                    ,'On [0-9]+[ -/]Jul[a-z]*','On [0-9]+[ -/]Aug[a-z]*','On [0-9]+[ -/]Sep[a-z]*'
                    ,'On [0-9]+[ -/]Oct[a-z]*','On [0-9]+[ -/]Nov[a-z]*','On [0-9]+[ -/]Dec[a-z]*'
                    ,'On [0-9]+[ -/][0-9]+[ -/][0-9]+']
reg_start = re.compile('|'.join(prev_mail_start))

def fetch_first_mail_text(mail_text,first_mail_flag = True):
    '''
    :param mail_text: mail texts
    :param first_mail_flag : if the text is containing entire mail chain or not
    :return: list of first mails
    '''
    sent = mail_text
    if not first_mail_flag:
        sent,_ = quotequail_mail_split(sent)
    match0 = reg_start.search(sent)
    if match0:
        sent = sent[:match0.start()]
    match1 = re.search(r'[\w]+ [\w]+[ ]*<[\w\.-]+@[\w\.-]+>',sent)
    if match1:
        sent = sent[:match1.start()]
    match2 = re.search(r'mailto:[\w\.-]+@[\w\.-]+',sent)
    if match2:
        sent = sent[:match2.start()]
    match3 = re.search(r'\([\w\.-]+@[\w\.-]+\)',sent)
    if match3:
        sent = sent[:match3.start()]
    match4 = re.search(r'<[\w\.-]+@[\w\.-]+',sent)
    if match4:
        sent = sent[:match4.start()]
    match5 = re.search(r'[\w]+[ ]*<[\w\.-]+@[\w\.-]+>',sent)
    if match5:
        sent = sent[:match5.start()]
    match6 = re.search(r'<[\w\.-]+@[\w\.-]+>',sent)
    if match6:
        sent = sent[:match6.start()]
    match7 = re.search(r'<[\w\.-]+@[\w\.-]+',sent)
    if match7:
        sent = sent[:match7.start()]
    match8 = re.search(r'From:',sent)
    if match8:
        sent = sent[:match8.start()]
    return sent

import pandas as pd,re
mail_split_reg = re.compile(r'On ((Sun|Mon|Tue|Wed|Thu|Fri|Sat)(.*)|([0-3]*))(.*)wrote:')

with open('vishnu_dotcom.txt','r') as f:
    lll = f.readlines()

ll = [eval(i) for i in lll]
ll = [i for i in ll if not re.search('@contractiq\.com',i[1])]
ldic = {'From':[],'Subject':[],'Content':[],'Time':[]}
for (id,sub,content,time) in ll:
    ldic['From'].append(id)
    ldic['Subject'].append(sub)
    ldic['Content'].append(content)
    ldic['Time'].append(time)

import datetime
import re
time_list = [i for i in ldic['Time']]
new_time_list = []
for i in time_list:
    try:
        time_val = datetime.datetime.strptime(re.sub(',','',i)[:30],'%a %d %b %Y %H:%M:%S %z')
    except:
        time_val = datetime.datetime.strptime(re.sub(',','',i),'%d %b %Y %H:%M:%S %z')
    new_time_list.append(time_val)

ldic['Time'] = new_time_list


# tmp = [re.sub('re: ','',re.IGNORECASE).strip() for i in ldic['Subject']]
ldic['Content'] = [mail_split_reg.split(re.sub('\n|\r',' ',fetch_first_mail_text(i)))[0] for i in ldic['Content']]
ldf = pd.DataFrame(ldic)
ldf = ldf.sort(['Time'])
ldf_1 = ldf.groupby(['From']).first().reset_index()
ldf_1.to_csv('vishnu_dotcom.csv',index=False,quoting=1)
