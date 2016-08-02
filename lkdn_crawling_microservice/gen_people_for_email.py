__author__ = 'joswin'

import pandas as pd
from optparse import OptionParser
import logging

from postgres_connect import PostgresConnect
from constants import desig_list_regex,designations_column_name

def gen_people_details(list_id,desig_list=None):
    '''
    :param list_id:
    :param desig_list:
    :return:
    '''
    logging.info('started generating people details for email generation')
    if not desig_list:
        desig_list_reg = desig_list_regex
    else:
        desig_list_reg = '\y' + '\y|\y'.join(desig_list) + '\y'
    con = PostgresConnect()
    con.get_cursor()
    table_name_id = str(list_id).split('-')[0]
    # all people urls and current company
    con.cursor.execute('drop table if exists crawler.tmp_table_email_gen_{}'.format(table_name_id))
    con.commit()
    query = " create table crawler.tmp_table_email_gen_{} as select list_id,list_items_url_id, "\
        " unnest(crawler.clean_linkedin_url_array(string_to_array(company_linkedin_url,'|'))) company_linkedin_url, "\
        "linkedin_url as people_linkedin_url from crawler.linkedin_people_base a where "\
        "company_linkedin_url like '%%linkedin%%' and a.list_id = %s ".format(table_name_id)
    con.cursor.execute(query,(list_id,))
    con.commit()
    # people_urls from current company
    # con.cursor.execute('drop table if exists crawler.tmp_table2_email_gen_{}'.format(list_id))
    # con.commit()
    query = " insert into crawler.tmp_table_email_gen_{} select list_id,list_items_url_id, "\
        " linkedin_url as company_linkedin_url, "\
        "unnest(crawler.clean_linkedin_url_array(crawler.extract_related_info(string_to_array(employee_details,'|'),1))) as people_linkedin_url "\
        "from crawler.linkedin_company_base a where "\
        "employee_details like '%%linkedin%%' and a.list_id = %s ".format(table_name_id)
    con.cursor.execute(query,(list_id,))
    con.commit()
    # creating index
    query = "create index on crawler.tmp_table_email_gen_{} (company_linkedin_url)".format(table_name_id)
    con.cursor.execute(query)
    query = "create index on crawler.tmp_table_email_gen_{} (people_linkedin_url)".format(table_name_id)
    con.cursor.execute(query)

    # joining both
    con.cursor.execute('drop table if exists crawler.tmp_table1_email_gen_{}'.format(table_name_id))
    con.commit()
    # below query takes a long to execute and has some problems
    query = "create table crawler.tmp_table1_email_gen_{} as "\
            "select a.name,a.sub_text,b.website,b.company_name,d.list_id,d.list_items_url_id from "\
            " crawler.linkedin_people_base a join crawler.linkedin_people_redirect_url c on a.linkedin_url = c.redirect_url "\
            "join crawler.tmp_table_email_gen_{} d on (c.redirect_url=d.people_linkedin_url or c.url = d.people_linkedin_url) "\
            "join crawler.linkedin_company_redirect_url e on (d.company_linkedin_url = e.url or d.company_linkedin_url = e.redirect_url) "\
            " join crawler.linkedin_company_base b on e.redirect_url = b.linkedin_url "\
            " where d.list_id = %s and a.sub_text ~* '{}' ".format(table_name_id,table_name_id,desig_list_reg)
    # query = "create table crawler.tmp_table1_email_gen_{} as "\
    #         "select a.name,a.sub_text,b.website,b.company_name,d.list_id,d.list_items_url_id from "\
    #         " crawler.linkedin_people_base a  "\
    #         "join crawler.tmp_table_email_gen_{} d on a.linkedin_url=d.people_linkedin_url "\
    #         " join crawler.linkedin_company_base b on d.company_linkedin_url = b.linkedin_url "\
    #         " where d.list_id = %s and a.sub_text ~* '{}' ".format(table_name_id,table_name_id,desig_list_reg)
    con.cursor.execute(query,(list_id,))
    con.commit()
    # create first name, middle name and last name
    con.cursor.execute('alter table crawler.tmp_table1_email_gen_{} add column name_cleaned text[], add column domain text'.format(table_name_id))
    con.commit()
    query = "update crawler.tmp_table1_email_gen_{} set name_cleaned = crawler.name_cleaner(name), "\
            "domain = replace(substring(website  from '.*://([^/]*)'),'www.','') ".format(table_name_id)
    con.cursor.execute(query)
    con.commit()
    query = "insert into crawler.people_details_for_email_verifier "\
            " (list_id,list_items_url_id,first_name,middle_name,last_name,domain,designation,company_name,company_website)  "\
            "select distinct list_id,list_items_url_id, name_cleaned[2] as first_name, name_cleaned[3] as middle_name, "\
            "name_cleaned[4] as last_name, domain, sub_text as designation,company_name, website as company_website "\
            " from crawler.tmp_table1_email_gen_{} where "\
            "domain not in ('http://','http://-','http://.','http://...','http://1','') and  "\
            "name_cleaned[2] is not null and name_cleaned[4] is not null and name_cleaned[2] not in('','.','..') "\
            " and name_cleaned[4] not in ('','.','..') and domain is not null and domain != 'NULL' "\
            "and list_id =%s "\
            " on conflict do nothing".format(table_name_id)
    con.cursor.execute(query,(list_id,))
    con.commit()
    # drop tables
    con.cursor.execute('drop table crawler.tmp_table1_email_gen_{}'.format(table_name_id))
    con.cursor.execute('drop table crawler.tmp_table_email_gen_{}'.format(table_name_id))
    con.commit()
    con.close_cursor()
    logging.info('generated people details for email generation')

if __name__ == "__main__":
    optparser = OptionParser()
    optparser.add_option('-n', '--name',
                         dest='list_name',
                         help='name of the list',
                         default=None)
    optparser.add_option('-d', '--designations',
                         dest='desig_loc',
                         help='location of csv containing target designations',
                         default=None)
    (options, args) = optparser.parse_args()
    list_name = options.list_name
    if not list_name:
        raise ValueError('need list name to run')
    desig_loc = options.desig_loc
    if desig_loc:
        inp_df = pd.read_csv(desig_loc)
        desig_list = list(inp_df[designations_column_name])
    else:
        desig_list = None
    con = PostgresConnect()
    con.get_cursor()
    con.execute("select id from crawler.list_table where list_name = %s",(list_name,))
    res = con.cursor.fetchall()
    con.close_cursor()
    if not res:
        raise ValueError('the list name given do not have any records')
    list_id = res[0][0]
    gen_people_details(list_id,desig_list)
