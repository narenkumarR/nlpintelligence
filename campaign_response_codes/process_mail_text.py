from email_finder import EmailConnectionService
def fetch_mails():
    ecs = EmailConnectionService()
    subjects = ['Scaling Engineering Teams','Scaling Mobile Initiatives for Brands','Mobile Report - ContractIQ',
        'Is your hiring plan on track?','Planning a conversation','Can we speak?','Include us in your 2015 planning',
        'Planning a meeting - 2nd week of Jan',"What's your plan for Tableau in 2015?",'Planning our conversation',
        'We should speak','Planning a conversation on digital innovation','Tech / M&A - Planning for a conversation',
        'Planning for a Conversation','Hello ! We are ContractIQ!','Conversation with you !',"Thank you. It's been a pleasure!",
        'A handy startup resource!','Tech & M&A Introductions','Game Development Teams - You might like this!',
        'Mobile aggregation - Can we speak?','Visiting California']
    for sub in subjects:
        print(sub)
        ecs.read_save_mails(search_string = '(SUBJECT "'+sub+'")' , out_file = 'suriya_mails.txt')

import quotequail

with open('to_srikant_1.txt','a') as outfile:
    with open('suriya_mails.txt','r') as infile:
        for line in infile:
            mm = list(eval(line))
            tmp=quotequail.quote(mm[2])
            prodic = {'quoted_part':[],'unquoted_part':[]}
            for flag,text in tmp:
                if len(text)>0:
                    if flag:
                        prodic['unquoted_part'].append(text)
                    else:
                        prodic['quoted_part'].append(text)
            mm.append(prodic)
            outfile.write(str(mm)+'\n')

mail_dict={}
with open('suriya_mails.txt','r') as infile:
    for line in infile:
        mm = list(eval(line))
        if (mm[0]) not in mail_dict.keys():
            mail_dict[(mm[0])] = {'mails':[mm[2]],'datetimes':[mm[3]],'subjects':[mm[1]]}
        else:
            prev_dets = mail_dict[(mm[0])]
            prev_dets['mails'].append(mm[2])
            prev_dets['datetimes'].append(mm[3])
            prev_dets['subjects'].append(mm[1])
            mail_dict[(mm[0])] = prev_dets

mail_dict = {'email':[],'subject':[],'time':[],'quoted_part':[],'unquoted_part':[],'full_text':[]}
with open('suriya_mails.txt','r') as infile:
    for line in infile:
        mm = eval(line)
        mail_dict['email'].append(mm[0])
        mail_dict['subject'].append(mm[1])
        mail_dict['time'].append(mm[3])
        mail_dict['full_text'].append(mm[2])
        quoted_part, unquoted_part = '',''
        tmp=quotequail.quote(mm[2])
        for flag,text in tmp:
            if len(text)>0:
                if flag:
                    unquoted_part += text
                else:
                    quoted_part += text
        mail_dict['quoted_part'].append(quoted_part)
        mail_dict['unquoted_part'].append(unquoted_part)


import pandas as pd,csv
mails = pd.DataFrame(mail_dict)
mails = mails[['email','subject','time','unquoted_part','quoted_part','full_text']]
mails1 = mails[~mails['email'].isin(['Ashwin Ramasamy <ashwin@contractiq.com>','Suriyah Krishnan <suriyah@contractiq.com>'])]

cond = mails['email'].str.contains('contractiq') | mails['subject'].str.lower().str.contains('automatic reply') | \
       mails['subject'].str.lower().str.contains('undeliverable') | mails['subject'].str.lower().str.contains('rejected') |\
       mails['subject'].str.lower().str.contains('out of office') | mails['subject'].str.lower().str.contains('Autoreply')

mails2 = mails.drop(mails[cond].index.values)
mails2 = mails2[~mails2['email'].isin(['Goonjan Mall <goonjanmall@gmail.com>'])]
mails2.to_csv('campaign_replies.csv',index=False,quoting=csv.QUOTE_NONNUMERIC,sep='$',escapechar="'")

##select one with the lowest length as the first mail? or earliest mail?
mail_dict_idwise = {}
with open('suriya_mails.txt','r') as infile:
    for line in infile:
        mm = eval(line)
        if (mm[0],mm[1]) not in mail_dict_idwise.keys():
            mail_dict_idwise[(mm[0],mm[1])] = {'texts':[mm[2]],'times':[mm[3]]}
        else:
            mail_dict_idwise[(mm[0],mm[1])]['texts'].append(mm[2])
            mail_dict_idwise[(mm[0],mm[1])]['times'].append(mm[3])
