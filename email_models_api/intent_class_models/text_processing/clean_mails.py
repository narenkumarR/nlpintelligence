#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'joswin'

import re,pdb
import nltk
import quotequail

from intent_class_models.text_processing import entity_extraction
st = entity_extraction.StanfordNERTaggerExtractor()

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


def clean_mail_text(a):
    '''
    :param a:mail text
    :return: cleaned text
    '''

    ########################################################################(removing signatures and from to etc)
    a=re.sub(r'From:[\s\w\W]+Subject:','',a)
    a=re.sub(r'Thank you,[\s\w\W]+Subject:','',a)
    a=re.sub(r'Thank you[.][\s\w\W]+Subject:','',a)
    a=re.sub(r'Thank you [\s\w\W]+Subject:','',a)
    a=re.sub(r'Thanks,[\s\w\W]+Subject:','',a)
    a=re.sub(r'Thanks[.][\s\w\W]+Subject:','',a)
    a=re.sub(r'Thanks [\s\w\W]+Subject:','',a)
    a=re.sub(r'Regards,[\s\w\W]+Subject:','',a)
    a=re.sub(r'Regards[.][\s\w\W]+Subject:','',a)
    a=re.sub(r'Regards [\s\w\W]+Subject:','',a)
    #a=re.sub(r'Thank you,[\s\w\W]+','',a)#EOT
    #a=re.sub(r'Thank you[.][\s\w\W]+','',a)#EOT
    #a=re.sub(r'Thank you [\s\w\W]+','',a)#EOT
    #a=re.sub(r'Thanks,[\s\w\W]+','',a)#EOT
    #a=re.sub(r'Thanks[.][\s\w\W]+','',a)#EOT
    #a=re.sub(r'Thanks [\s\w\W]+','',a)#EOT
    a=re.sub(r'Regards,[\s\w\W]+','',a)#EOT
    a=re.sub(r'Regards[.][\s\w\W]+','',a)#EOT
    #a=re.sub(r'Regards [\s\w\W]+','',a)#EOT

    #############################################################(time removal)
    #a=re.sub(r'[0-9]+:[0-9]+ AM','',a)
    #a=re.sub(r'[0-9]+:[0-9]+ PM','',a)

    #############################################################(email removal)

    a=re.sub(r'[\w\.-]+@[\w\.-]+',' ',a)

    #########################################################################(city_state_zip removal)

    a=re.sub(r'[a-zA-Z]+, [a-zA-Z]{2} - ([0-9]{5} | [0-9]{9})','',a)

    #########################################################################(phonenumber removal)

    a=re.sub(r'[0-9]+-[0-9]+-[0-9]+','',a)
    a=re.sub(r'([0-9]+) [0-9]+-[0-9]+','',a)

    a=re.sub(r'[0-9]+-[0-9]+','',a)
    a=re.sub(r'[0-9]+.[0-9]+.[0-9]+','',a)

    ############################################################################(url removal)

    a = re.sub(r'''(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'".,<>?«»“”‘’]))''', '', a,flags=re.MULTILINE)
    a=re.sub(r'^https?:\/\/.*[\r\n]*', '', a, flags=re.MULTILINE)
    a=re.sub(r'^http?:\/\/.*[\r\n]*', '', a, flags=re.MULTILINE)

    a=re.sub('<>','',a)
    ############################################################################(numbers removal)

    a=re.sub('(^| )([0-9]+)( |$)',' ',a)

    #######################################################################################(special characters removal)
    #a=re.sub(r'[^a-zA-Z0-9."_"," "]', '', a)
    # a = re.sub('\\n',',',a)
    a = re.sub(u"(\u2018|\u2019)","'",a)
    a = re.sub(r"[^a-zA-Z0-9\n'._, !-]", ' ', a)
    #######################################################################################(extra white spaces removal)
    a=re.sub(r' +',' ',a)

    #removing few more characters
    # a = re.sub(',+',',',a)
    # a = re.sub(',\.','. ',a)
    # a = re.sub(', +',',',a)
    a = re.sub('\\n+','\\n',a)
    a = re.sub(' +',' ',a)
    a = re.sub("_+",'_',a)
    a = re.sub("!+","!",a)
    a = re.sub("'+","'",a)
    a = re.sub("-+","-",a)
    a = re.sub("[.]+",".",a)
    return a


# def remove_end_text_mail(text):
#     ''' start from 3rd sentence. find names. check for thanks,regards,best,luck,wishes in the previous 3 words
#     if no name is present with above condition, remove everything from the last name '''
#     tags = st.tag_text_multi(text)
#     if len(tags) < 3:
#         return text
#     # tags = tags[2:]
#     found_flag = False
#     for sent_ind in range(2,len(tags)):
#         pdb.set_trace()
#         sent = tags[sent_ind]
#         # sent = [(wrd,tag) for wrd,tag in sent if wrd not in ['.',',','!','?']]
#         for wrd_ind in range(len(sent)):
#             wrd,tag = sent[wrd_ind]
#             if tag == 'PERSON':
#                 for ind1 in range(max(wrd_ind-4,0),wrd_ind):
#                     if sent[ind1][0].lower() in ['thanks','regards','best','luck','wishes','cheers']:
#                         pdb.set_trace()
#                         found_flag = True
#                         found_wrd_ind = ind1
#                         found_sent_ind = sent_ind
#                         break
#                 if not found_flag:
#                     prev_sent = tags[sent_ind-1]
#                     prev_sent_len = len(prev_sent)
#                     for ind1 in range(max(prev_sent_len-2,0),prev_sent_len):
#                         if prev_sent[ind1][0].lower() in ['thanks','regards','best','luck','wishes','cheers']:
#                             pdb.set_trace()
#                             found_flag = True
#                             found_wrd_ind = ind1
#                             found_sent_ind = sent_ind-1
#                             break
#             if found_flag:
#                 break
#         if found_flag:
#             break
#     if not found_flag:
#         for rev_sent_ind in range(-1,-1*(len(tags)-1),-1):
#             sent = tags[rev_sent_ind]
#             for rev_wrd_ind in range(-1,-1*(len(sent)+1),-1):
#                 wrd,tag = sent[rev_wrd_ind]
#                 if tag == 'PERSON':
#                     found_flag = True
#                     found_wrd_ind = len(sent)+rev_wrd_ind
#                     # break
#             if found_flag:
#                 found_sent_ind = len(tags)+rev_sent_ind
#                 break
#     if found_flag:
#         text1 = ''
#         for ind in range(found_sent_ind):
#             text1 += ' '.join([wrd for wrd,_ in tags[ind]])
#             text1 += ' '
#         last_sent = ' '.join([wrd for wrd,_ in tags[found_sent_ind][:found_wrd_ind]])
#         text1 += last_sent
#         return text1
#     else:
#         return text

def remove_endtext_ner_mail_listinput(mail_list,sender_names=None):
    '''Takes a list of sentences as input'''
    tags_all_split = get_stanford_tags_listinput(mail_list)
    out_list = []
    for tag_list in tags_all_split:
        tags = st.tag_text_multi_from_single(tag_list)
        tags = remove_endtext_tags(tags)
        tags = remove_ner_fromtag(tags,sender_names)
        out_list.append(get_sent_fromtag(tags))
    return out_list

def get_stanford_tags_listinput(mail_list):
    ''' '''
    list_split_code = '. spl_cd_X3ffty. '
    all_text = list_split_code.join(mail_list)
    all_text = ' '.join(nltk.word_tokenize(all_text))
    tags_all = st.tag_text_single(all_text)
    #above list is a single sentence. Now split them into multiple based on the list_split_code
    tags_all_split = []
    start = 0
    start_list = []
    for wrd,tag in tags_all:
        if wrd == 'spl_cd_X3ffty':
            tags_all_split.append(start_list[1:-1])
            start_list = []
        else:
            start_list.append((wrd,tag))
    tags_all_split.append(start_list)
    return tags_all_split

def remove_endtext_ner_mail(text,sender_names=None):
    ''' remove end text and ner tags from text using StanfordNERTaggerExtractor'''
    tags = st.tag_text_multi(text)
    tags = remove_endtext_tags(tags)
    tags = remove_ner_fromtag(tags,sender_names)
    return get_sent_fromtag(tags)

def get_sent_fromtag(tags):
    text = ''
    for sent in tags:
        for wrd,_ in sent:
            text += wrd+' '
    return text

def remove_ner_fromtag(tags,sender_names = None):
    '''output of st.tag_text_multi is passed and returns a tag output where ner tags are removed'
    sender_names is a list of sender names in lower case. eg: ['suriyah','krishnan','ashwin','ramaswamy']'''
    tags = [[(wrd,tag) for wrd,tag in sent if tag=='O'] for sent in tags]
    if sender_names:
        tags = [[(wrd,tag) for wrd,tag in sent if wrd.lower() not in sender_names] for sent in tags]
    return tags

mail_end_words = ['thanks','regards','best','luck','wishes','cheers']
def remove_endtext_tags(tags):
    ''' takes stanford tags as input and removes bottom part . uses same logic in the remove_end_text_mail function
         '''
    # tags = st.tag_text_multi(text)
    if len(tags) < 3:
        return tags
    # tags = tags[2:]
    found_flag = False
    for sent_ind in range(2,len(tags)):
        sent = tags[sent_ind]
        # sent = [(wrd,tag) for wrd,tag in sent if wrd not in ['.',',','!','?']]
        for wrd_ind in range(len(sent)):
            wrd,tag = sent[wrd_ind]
            if tag == 'PERSON':
                for ind1 in range(max(wrd_ind-4,0),wrd_ind):
                    if sent[ind1][0].lower() in mail_end_words:
                        found_flag = True
                        found_wrd_ind = ind1
                        found_sent_ind = sent_ind
                        break
                if not found_flag:
                    prev_sent = tags[sent_ind-1]
                    prev_sent_len = len(prev_sent)
                    for ind1 in range(max(prev_sent_len-2,0),prev_sent_len):
                        if prev_sent[ind1][0].lower() in mail_end_words:
                            found_flag = True
                            found_wrd_ind = ind1
                            found_sent_ind = sent_ind-1
                            break
            if found_flag:
                break
        if found_flag:
            break
    if not found_flag:
        for rev_sent_ind in range(-1,-1*(len(tags)),-1):
            sent = tags[rev_sent_ind]
            for rev_wrd_ind in range(-1,-1*(len(sent)+1),-1):
                wrd,tag = sent[rev_wrd_ind]
                if tag == 'PERSON':
                    found_flag = True
                    found_wrd_ind = len(sent)+rev_wrd_ind
                    # break
            if found_flag:
                found_sent_ind = len(tags)+rev_sent_ind
                break
    if found_flag:
        tags1 = []
        for ind in range(found_sent_ind):
            tags1.append([(wrd,tag) for wrd,tag in tags[ind]])
        last_tag = [(wrd,tag) for wrd,tag in tags[found_sent_ind][:found_wrd_ind]]
        tags1.append(last_tag)
        return tags1
    else:
        return tags
