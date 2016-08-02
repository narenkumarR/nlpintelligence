__author__ = 'joswin'

import pandas as pd
import logging
import time
from requests.exceptions import HTTPError

from api_analysis import CrunchBaseExtra
from optparse import OptionParser
from postgres_connect import PostgresConnect

def get_news_for_companies(api_key,csv_loc,outfile = 'news_from_cb.csv',logfile='log_file.log'):
    '''
    :param api_key:
    :param csv_loc:
    :param logfile:
    :return:
    '''
    logging.basicConfig(filename=logfile, level=logging.INFO,format='%(asctime)s %(message)s')
    cb = CrunchBaseExtra(api_key)
    con = PostgresConnect()
    df = pd.read_csv(csv_loc)
    if 'company_name' not in df.columns:
        raise ValueError('Need to have a column as company_name')
    company_names = list(df['company_name'])
    company_reg = '|'.join([i.lower().strip() for i in company_names])
    con.get_cursor()
    query = "select distinct a.uuid,split_part(a.cb_url,'organization/',2) cb_username,company_name from "\
                " crunchbase_data.organizations a "\
                " where company_name ~* %s or split_part(a.cb_url,'organization/',2) ~* %s"
    con.execute(query,(company_reg,company_reg,))
    res = con.cursor.fetchall()
    if not res:
        raise ValueError('Could not find any companies in the csv file in the database. Try checking manually')
    with open(outfile,'w') as f:
        f.write('company_name,title,author,url,posted_on\n')
    for org_uuid,cb_username,company_name in res:
        logging.info('starting for cb_username : {}'.format(cb_username))
        try:
            news_list = cb.news_list(cb_username,{'sort_order':'posted_on DESC'})
        except HTTPError:
            logging.exception('http error for {}. Sleep for {} seconds'.format(cb_username,10))
            time.sleep(10)
            continue
        except KeyboardInterrupt:
            break
        except:
            logging.exception('Unanticipated error')
            continue
        if news_list:
            news_for_insert = [(org_uuid,i[0],i[1],i[2],i[3],i[4])  for i in news_list ]
            records_list_template = ','.join(['%s']*len(news_for_insert))
            insert_query = "INSERT INTO {} (org_uuid,title,author,url,posted_on,uuid) VALUES {} ".format('crunchbase_data.news',records_list_template)
            con.cursor.execute(insert_query, news_for_insert)
            con.commit()
            with open(outfile,'a') as f:
                for i in news_list:
                    f.write('{},{},{},{},{}\n'.format(company_name,i[0],i[1],i[2],i[3]))

if __name__ == '__main__':
    optparser = OptionParser()
    optparser.add_option('-k', '--key',
                         dest='api_key',
                         help='api key',
                         default=None)
    optparser.add_option('-l', '--logfile',
                         dest='logfile',
                         help='name of logfile',
                         default='log_file.log')
    optparser.add_option('-f', '--csvfile',
                         dest='csvfile',
                         help='name of csvfile',
                         default=None)
    optparser.add_option('-o', '--outfile',
                         dest='outfile',
                         help='name of output csvfile',
                         default='news_from_cb.csv')
    (options, args) = optparser.parse_args()
    api_key = options.api_key
    logfile = options.logfile
    csvfile = options.csvfile
    outfile = options.outfile
    if not csvfile:
        raise ValueError('Need to provide csv location')
    get_news_for_companies(api_key,csvfile,outfile,logfile)
