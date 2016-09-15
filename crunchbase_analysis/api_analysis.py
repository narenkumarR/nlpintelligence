__author__ = 'joswin'

import pdb
from pycrunchbase import CrunchBase
from pycrunchbase.resource import News,Website
from optparse import OptionParser

class CrunchBaseExtra(CrunchBase):
    def __init__(self,api_key):
        CrunchBase.__init__(self,api_key)

    def news_list(self, permalink,params=None):
        """Get the details of news given a company's permalink

        Returns:
            iterator of news items (
        """
        node_data = self.get_node('organizations', permalink+'/news',params=params)
        if 'items' in node_data:
            news = []
            for item in node_data['items']:
                news_obj = News(item)
                if news_obj.title or news_obj.url:
                    news.append((news_obj.title,news_obj.author,news_obj.url,news_obj.posted_on,news_obj.uuid))
            return news
        elif 'item' in node_data:
            news_obj = News(node_data['item'])
            return [(news_obj.title,news_obj.author,news_obj.url,news_obj.posted_on,news_obj.uuid)]
        else:
            return []

    def websites_list(self,permalink,params=None):
        """Get the details of websites given a company's permalink
        Returns:
            list of websites
        """
        node_data = self.get_node('organizations', permalink+'/websites',params=params)
        if 'items' in node_data:
            websites = []
            for item in node_data['items']:
                web_obj = Website(item)
                if web_obj.url:
                    websites.append((web_obj.url,web_obj.website_type,web_obj.uuid))
            return websites
        elif 'item' in node_data:
            web_obj = Website(node_data['item'])
            return [(web_obj.url,web_obj.website_type,web_obj.uuid)]
        else:
            return []

# cb = CrunchBaseExtra(api_key)

# company_cb_name = 'flipkart'
# company_news = cb.news(company_cb_name)

'''
create table news  (
    org_uuid text,
    title text,
    author text,
    url text,
    posted_on timestamp,
    uuid text
    );
create table websites  (
    org_uuid text,
    url text,
    website_type text,
    uuid text
    );

'''

from postgres_connect import PostgresConnect
from requests.exceptions import HTTPError
import logging
import time
import datetime
import pdb

def get_news_websites(api_key,start_ind=0,chunk_size=None,api_type=0,logfile='log_file.log'):
    '''
    :param api_key:
    :param start_ind:
    :param chunk_size:
    :return:
    '''
    logging.basicConfig(filename=logfile, level=logging.INFO,format='%(asctime)s %(message)s')
    logging.info('started at {}'.format(datetime.datetime.now()))
    con = PostgresConnect()
    cb = CrunchBaseExtra(api_key)
    ind,ind1, errors = 0, int(start_ind), 0
    con.get_cursor()
    #pdb.set_trace()
    while True:
        if chunk_size:
            # query = "select distinct a.uuid,split_part(a.cb_url,'organization/',2) cb_username from crunchbase_data.organizations a "\
            #         " left join crunchbase_data.websites b on a.uuid=b.org_uuid left join crunchbase_data.websites c on a.uuid=c.org_uuid "\
            #         " where b.org_uuid is null and c.org_uuid is null and funding_rounds<'5' and country_code in ('USA','IND')  "\
            #         "  offset {} limit {}".format(start_ind+ind*chunk_size,chunk_size)
            query = "select distinct a.uuid,split_part(a.cb_url,'organization/',2) cb_username from crunchbase_data.organizations a "\
                    " where short_description ~* '\ysaas\y'   "\
                    "  offset {} limit {}".format(start_ind+ind*chunk_size,chunk_size)
        else:
            # query = "select uuid,cb_username from (select distinct a.uuid,split_part(a.cb_url,'organization/',2) cb_username,last_funding_on from crunchbase_data.organizations a "\
            #         " left join crunchbase_data.websites b on a.uuid=b.org_uuid left join crunchbase_data.websites c on a.uuid=c.org_uuid "\
            #         " where b.org_uuid is null and c.org_uuid is null and funding_rounds<'5' and country_code in ('USA','IND')  "\
            #         " order by last_funding_on desc)a offset {} ".format(start_ind,chunk_size)
            query = "select uuid,cb_username from (select distinct a.uuid,split_part(a.cb_url,'organization/',2) cb_username,last_funding_on from "\
                    " crunchbase_data.organizations a "\
                    " where  short_description ~* '\ysaas\y'  "\
                    " order by last_funding_on desc)a offset {} ".format(start_ind)
        con.execute(query)
        res = con.cursor.fetchall()
        if not res:
            break
        for org_uuid,cb_username in res:
            logging.info('starting for cb_username : {}, ind1: {}'.format(cb_username,ind1))
            ind1 += 1
            try:
                if api_type == 0 or api_type == 1:
                    news_list = cb.news_list(cb_username,{'sort_order':'posted_on DESC'})
                if api_type == 0 or api_type == 2:
                    websites_list = cb.websites_list(cb_username)
            except HTTPError:
                errors += 1
                logging.exception('server blocked. Sleep for {} seconds'.format(errors*60))
                time.sleep(errors*60)
                continue
            except KeyboardInterrupt:
                break
            except:
                logging.exception('Unanticipated error')
                continue
            errors = 0
            if api_type == 0 or api_type == 1:
                if news_list:
                    news_for_insert = [(org_uuid,i[0],i[1],i[2],i[3],i[4])  for i in news_list ]
                    records_list_template = ','.join(['%s']*len(news_for_insert))
                    insert_query = "INSERT INTO {} (org_uuid,title,author,url,posted_on,uuid) VALUES {} ".format('crunchbase_data.news',records_list_template)
                    con.cursor.execute(insert_query, news_for_insert)
                    con.commit()
            if api_type == 0 or api_type == 2:
                if websites_list:
                    websites_for_insert = [(org_uuid,i[0],i[1],i[2])  for i in websites_list ]
                    records_list_template = ','.join(['%s']*len(websites_for_insert))
                    insert_query = "INSERT INTO {} (org_uuid,url,website_type,uuid) VALUES {} ".format('crunchbase_data.websites',records_list_template)
                    con.cursor.execute(insert_query, websites_for_insert)
                    con.commit()
        ind += 1
    con.close_cursor()
    logging.info('stopped at {}'.format(datetime.datetime.now()))

if __name__ == '__main__':
    optparser = OptionParser()
    optparser.add_option('-k', '--key',
                         dest='api_key',
                         help='api key',
                         default=None)
    optparser.add_option('-s', '--start',
                         dest='start_ind',
                         help='start ind for organization table',
                         default=0)
    optparser.add_option('-c', '--chunk',
                         dest='chunk_size',
                         help='chunk size for processing',
                         default=None)
    optparser.add_option('-t', '--type',
                         dest='api_type',
                         help='type of api request: 1 for news 2 for websites 0 for both',
                         default=0,type='float')
    optparser.add_option('-l', '--logfile',
                         dest='logfile',
                         help='name of logfile',
                         default='log_file.log')
    (options, args) = optparser.parse_args()
    api_key = options.api_key
    start_ind = options.start_ind
    chunk_size = options.chunk_size
    api_type = options.api_type
    logfile = options.logfile
    get_news_websites(api_key,start_ind,chunk_size,api_type,logfile)
