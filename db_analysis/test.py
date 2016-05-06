__author__ = 'joswin'

import os,re
import pymongo
from pymongo import MongoClient

client = MongoClient()
db = client.linkedin_data
company_data = db.organization_data
people_data = db.people_data
# company_data.create_index([('LinkedinURL',pymongo.TEXT)],unique=True,background=True)
# people_data.create_index([('LinkedinURL',pymongo.TEXT)],unique=True,background=True)

def get_files_in_dir(dir_path='.',remove_regex = '',match_regex=''):
    ''' get matching files
    :param dir_path:
    :param remove_regex:
    :param match_regex:
    :return:
    '''
    files = os.listdir(dir_path)
    if dir_path == '.':
        dir_path = ''
    if remove_regex:
        files = [i for i in files if not re.search(remove_regex,i)]
    if match_regex:
        files = [i for i in files if re.search(match_regex,i)]
    files = [dir_path+i for i in files]
    return files

crawled_loc = '/home/madan/Desktop/joswin_bck/toPendrive/works/nlp-intelligence/crawling/crawled_res/'
crawled_files_company = get_files_in_dir(crawled_loc,match_regex='^company.+\.txt$')
crawled_files_people = get_files_in_dir(crawled_loc,match_regex='^people.+\.txt$')

def get_docs_from_files(file_names):
    for f_name in file_names:
        with open(f_name,'r') as f:
            for line in f:
                yield eval(line)

import logging
logging.basicConfig(filename='logfile.log',level=logging.INFO,format='%(asctime)s %(message)s')
logging.info('started execution')
for line_dic in get_docs_from_files(crawled_files_company):
    line_dic['LinkedinURL_text'] = line_dic['Linkedin URL']
    line_dic.pop('Linkedin URL',None)
    try:
        company_data.insert_one(line_dic)
    except pymongo.errors.DuplicateKeyError:
        logging.info('duplicate key error for url:{}'.format(line_dic['LinkedinURL_text']))
    except:
        logging.exception('error happened for url:{}'.format(line_dic['LinkedinURL_text']))

for line_dic in get_docs_from_files(crawled_files_people):
    line_dic['LinkedinURL_text'] = line_dic['Linkedin URL']
    line_dic.pop('Linkedin URL',None)
    try:
        people_data.insert_one(line_dic)
    except pymongo.errors.DuplicateKeyError:
        logging.info('duplicate key error for url:{}'.format(line_dic['LinkedinURL_text']))
    except:
        logging.exception('error happened for url:{}'.format(line_dic['LinkedinURL_text']))


# cursor = company_data.find()

cursor = company_data.aggregate(
    [
        {"$group": {"_id": "$Type", "count": {"$sum": 1}}}
    ]
)
res = []
for document in cursor:
    # print(document)
    res.append(document)

