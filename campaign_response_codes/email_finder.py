"""
File : email_finder.py
Created On: 23-Sep-2015
Author: ideas2it
"""
import datetime
import email
import imaplib
import re
import itertools
import smtplib
import time
import getpass
# from queue import Queue
# from threading import Thread

from email_details import username, password,imap_server,subjects,subjects_matcher


class EmailConnectionService(object):
    def __init__(self,inbox_folder = None):
        self.connect(inbox_folder)

    def connect(self,inbox_folder = None):
        try:
            self.conn.check()
        except:
            self.conn = imaplib.IMAP4_SSL(imap_server)
            try:
                self.conn.login(username, password)
            except:
                print('Login Failure')
        if inbox_folder is None:
            self.conn.select(readonly=True)
        else:
            self.conn.select(inbox_folder,readonly=True)
        self.connected_folder = inbox_folder

    def disconnect(self):
        self.conn.close()
        self.conn.logout()

    def reconnect(self,inbox_folder = None):
        if self.connected_folder == inbox_folder:
            try:
                self.conn.check()
            except :
                self.connect(inbox_folder = inbox_folder)
        else:
            self.connect(inbox_folder = inbox_folder)        

    # def read_mails(self):
    #     try:
    #         result, data = self.conn.uid('search', None, '(UNSEEN)')
    #         # uids = data[0].split()
    #         uids = data[0].decode('ascii').split()
    #
    #         #fetching all uids together
    #         fetch_ids = ','.join([i for i in uids])
    #         result_all, data_all = self.conn.uid('fetch', fetch_ids, '(RFC822.HEADER BODY.PEEK[1])')
    #         for ind in range(int(len(data_all)/3)):
    #             raw_email = data_all[ind*3][1]+data_all[ind*3+1][1]
    #             email_message = email.message_from_string(raw_email.decode('ascii'))
    #             print(email_message)
    #         # print('yielding')
    #         # yield str(actual_addresses)
    #     except Exception as inst:
    #         print('Exception:',inst)

    def read_mails(self,search_string = '(UNSEEN)',inbox_folder = None,subjects_list = []):
        filter_subject = True if len(subjects_list)>0 else False
        self.mails = []
        self.reconnect(inbox_folder = inbox_folder)
        # n = 0
        (retcode, messages) = self.conn.search(None, search_string)
        unread_msg_nums = messages[0].split()
        if retcode == 'OK':
            try:
                for num in messages[0].split():
                    # n += 1
                    # typ, data = self.conn.fetch(num,'(RFC822)')
                    typ, data = self.conn.fetch(num,'(RFC822 BODY.PEEK[])')
                    # print(data)
                    for response_part in data:
                        if isinstance(response_part, tuple):
                            original = email.message_from_string(response_part[1].decode('ascii','replace'))
                            if (not filter_subject) or (original['Subject'] in subjects_list):
                                #date_tuple will be in format where last element is time difference with utc in minutes
                                # date_tuple = email.utils.parsedate_tz(original['Date'])
                                # local_date = datetime.datetime.now()
                                # if date_tuple:
                                #     local_date = datetime.datetime.fromtimestamp(
                                #         email.utils.mktime_tz(date_tuple))
                                    # print "Local Date:", \
                                    #     local_date.strftime("%a, %d %b %Y %H:%M:%S")
                                #we will pass the date in the original form itself
                                date_str = ''
                                if original['Date']:
                                    date_str = original['Date']
                                # print('XXXX message from: XXXX',original['From'])
                                # print('XXXX subject XXXX',original['Subject'])
                                if original.get_content_maintype() == 'multipart': #If message is multi part we only want the text version of the body, this walks the message and gets the body.
                                    for part in original.walk():
                                        if part.get_content_type() == "text/plain":
                                            body = part.get_payload(decode=True)
                                            # print('XXXX body XXXX',body)
                                            self.mails.append((original['From'],original['Subject'],body,date_str))
                                        else:
                                            continue
                            else:
                                continue
                    # typ, data = self.conn.store(num,'-FLAGS','\\Seen') ##not working

            except Exception as inst:
                print('Exception:',inst)
        print('making read')
        for e_id in unread_msg_nums:
            # print(e_id)
            # self.conn.store(e_id, '+FLAGS', '\Seen')
            self.conn.store(e_id, '-FLAGS','\\Seen')
        # # print(self.mails)

    def read_save_mails(self,search_string = 'ALL',inbox_folder = None, out_file = 'Mails.txt',subjects_list = []):
        filter_subject = True if len(subjects_list)>0 else False
        self.reconnect(inbox_folder = inbox_folder)
        # n = 0
        messages = []
        for sub in subjects_list:
            retcode, messages_tmp = self.conn.search(None, 'SUBJECT',sub)
            messages.extend(messages_tmp[0].split())
        # unread_msg_nums = messages[0].split()
        unread_msg_nums = messages
        # (retcode, messages) = self.conn.search(None, search_string)
        # unread_msg_nums = messages[0].split()
        print(len(unread_msg_nums))
        if retcode == 'OK':
            with open(out_file, "a") as myfile:
                for num in unread_msg_nums:
                    try:
                        print(num),type(num)
                        # n += 1
                        typ, data = self.conn.fetch(num,'(RFC822)')
                        # self.conn.uid('STORE', num, '-FLAGS', '\SEEN')
                        # print(data)
                        for response_part in data:
                            if isinstance(response_part, tuple):
                                original = email.message_from_string(response_part[1])
                                if (not filter_subject) or (original['Subject'] in subjects_list) or subjects_matcher.search(original['Subject']):
                                    #date_tuple will be in format where last element is time difference with utc in minutes
                                    # date_tuple = email.utils.parsedate_tz(original['Date'])
                                    # local_date = datetime.datetime.now()
                                    # if date_tuple:
                                    #     local_date = datetime.datetime.fromtimestamp(
                                    #         email.utils.mktime_tz(date_tuple))
                                        # print "Local Date:", \
                                        #     local_date.strftime("%a, %d %b %Y %H:%M:%S")
                                    #we will pass the date in the original form itself

                                    date_str = ''
                                    if original['Date']:
                                        date_str = original['Date']
                                    # print('XXXX message from: XXXX',original['From'])
                                    # print('XXXX subject XXXX',original['Subject'])
                                    # if original.get_content_maintype() == 'multipart': #If message is multi part we only want the text version of the body, this walks the message and gets the body.
                                    for part in original.walk():
                                        if part.get_content_type() == "text/plain":
                                            body = part.get_payload(decode=True)
                                            # print('XXXX body XXXX',body)
                                            myfile.write(str((original['From'],original['Subject'],body,date_str))+'\n')
                                        else:
                                            continue
                                else:
                                    continue
                        # typ, data = self.conn.store(num,'-FLAGS','\\Seen') ##not working

                    except Exception as inst:
                        print('Error happened')
                        self.reconnect(inbox_folder = inbox_folder)
        # print('making read')
        # for e_id in unread_msg_nums:
        #     # print(e_id)
        #     # self.conn.store(e_id, '+FLAGS', '\Seen')
        #     self.conn.store(e_id, '-FLAGS','\\Seen')
        # # print(self.mails)
        # # self.disconnect()

if __name__== '__main__':
    ecs = EmailConnectionService()
    # ecs.read_save_mails(search_string='(SUBJECT "{}")'.format('" OR "'.join(subjects)),subjects_list=subjects)
    ecs.read_save_mails(subjects_list=subjects)