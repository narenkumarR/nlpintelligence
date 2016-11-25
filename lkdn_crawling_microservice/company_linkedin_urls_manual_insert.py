__author__ = 'joswin'

import pandas as pd
import re
from optparse import OptionParser

from postgres_connect import PostgresConnect
from constants import company_name_field,company_details_field,linkedin_url_column
from constants import company_urls_to_crawl_priority_table,company_finished_urls_table

def upload_url_list(csv_loc=None,list_name=None):
    '''
    :param csv_loc:
    :return:
    '''
    if csv_loc is None:
        raise ValueError('give location of csv with linkedin urls. Col name '\
                'should be {},{},{}'.format(company_name_field,company_details_field,linkedin_url_column))
    if list_name is None:
        raise ValueError('Need list name')
    # url_df = pd.read_csv(csv_loc)
    url_df = pd.read_csv(csv_loc,sep=None)
    url_df = url_df.fillna('')
    url_list = list(url_df[linkedin_url_column])
    if url_list:
        url_list = ['https://www.linkedin.com/company/'+re.split('/',re.split(r'linkedin\.com/',url)[-1])[1]
                        if url else ''  for url in url_list ]
    company_dets = [(url_df.iloc[i][company_name_field],url_df.iloc[i][company_details_field]) for i in range(url_df.shape[0])]
    con = PostgresConnect()
    con.get_cursor()
    con.execute("select id from crawler.list_table where list_name = %s",(list_name,))
    res = con.cursor.fetchall()
    # con.close_cursor()
    if not res:
        # raise ValueError('the list name given do not have any records')
        query = " insert into crawler.list_table (list_name) values (%s) "
        con.execute(query,(list_name,))
        con.execute("select id from crawler.list_table where list_name = %s",(list_name,))
        res = con.cursor.fetchall()
    list_id = res[0][0]
    # prob with getting correct list_items_id while inserting- insert the name to list_item table
    if url_list and company_dets:
        records_list_template = ','.join(['%s']*len(company_dets))
        insert_query = "INSERT INTO {} (list_id,list_input,list_input_additional,url_extracted) VALUES {} "\
                        "ON CONFLICT DO NOTHING".format('crawler.list_items',records_list_template)
        urls_to_crawl1 = [(list_id,i[0],i[1],0) for i in company_dets]
        con.cursor.execute(insert_query, urls_to_crawl1)
        con.commit()
        # now insert urls directly into the list_items_urls table
        for url,name_val in zip(url_list,company_dets):
            try:
                if not url:
                    continue
                name = name_val[0]
                value = name_val[1]
                query = "insert into crawler.list_items_urls (list_id,list_items_id,url) "\
                        " select  list_id,id as list_items_id,%s as url "\
                        " from {list_items_table} where list_input = %s and list_input_additional = %s " \
                        " and list_id = %s "\
                        " on conflict do nothing ".format(list_items_table='crawler.list_items')
                con.cursor.execute(query,(url,name,value,list_id,))
                con.commit()
                # update the url_extracted column also
                query = " update {list_items_table} set url_extracted = 1 " \
                        " where list_id = %s and list_input = %s and list_input_additional = %s  ".format(
                    list_items_table='crawler.list_items'
                )
                con.cursor.execute(query,(list_id,name,value,))
                con.commit()
            except Exception,e:
                print('error happened for url:{},name_val:{}'.format(url,name_val))
                print(e)
                try:
                    con.close_cursor()
                except:
                    pass
                con.get_cursor()
        query = "insert into {} (url,list_id,list_items_url_id) select a.url,a.list_id,a.id as list_items_url_id "\
                    " from {} a left join {} b on a.list_id=b.list_id and a.id = b.list_items_url_id where a.list_id = %s "\
                    " and b.list_id is NULL "\
                    " on conflict do nothing".format(company_urls_to_crawl_priority_table,'crawler.list_items_urls',company_finished_urls_table)
        con.cursor.execute(query,(list_id,))
        con.commit()
    con.close_cursor()

if __name__ == "__main__":
    optparser = OptionParser()
    optparser.add_option('-n', '--name',
                         dest='list_name',
                         help='name of the list',
                         default=None)
    optparser.add_option('-f', '--csvfile',
                         dest='url_csv',
                         help='location of csv with linkedin urls',
                         default=None)
    (options, args) = optparser.parse_args()
    list_name = options.list_name
    csv_loc = options.url_csv
    if not list_name:
        raise ValueError('need list name to run')
    if not csv_loc:
        raise ValueError('give location of csv with urls')
    upload_url_list(csv_loc,list_name)

