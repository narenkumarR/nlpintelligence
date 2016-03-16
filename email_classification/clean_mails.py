#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'joswin'

import re
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

prev_mail_start = ['On Sun[ ,]+[\w0-9]+','On Mon','On Tue','On Wed','On Thu','On Fri','On Sat'\
                   ,'On Jan[ -/][0-9]+','On Feb[ -/][0-9]+','On Mar[ -/][0-9]+','On Apr[ -/][0-9]+','On May[ -/][0-9]+','On Jun[ -/][0-9]+'\
                    ,'On Jul[ -/][0-9]+','On Aug[ -/][0-9]+','On Sep[ -/][0-9]+','On Oct[ -/][0-9]+','On Nov[ -/][0-9]+','On Dec[ -/][0-9]+'
                   ,'On [0-9]+[ -/]Jan','On [0-9]+[ -/]Feb','On [0-9]+[ -/]Mar','On [0-9]+[ -/]Apr','On [0-9]+[ -/]May','On [0-9]+[ -/]Jun'\
                    ,'On [0-9]+[ -/]Jul','On [0-9]+[ -/]Aug','On [0-9]+[ -/]Sep','On [0-9]+[ -/]Oct','On [0-9]+[ -/]Nov','On [0-9]+[ -/]Dec'
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

    a = re.sub(r'''(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'".,<>?«»“”‘’]))''', '', a)
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

    return a
