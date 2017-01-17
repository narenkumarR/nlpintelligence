__author__ = 'joswin'

import pandas as pd
from optparse import OptionParser
import logging

from postgres_connect import PostgresConnect
from constants import desig_list_regex,designations_column_name

def gen_people_details(list_id,desig_list=None,company_base_table = 'crawler.linkedin_company_base',
                       people_base_table = 'crawler.linkedin_people_base'):
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
    table_name_id = '_'.join(str(list_id).split('-'))
    # all people urls and current company
    con.cursor.execute('drop table if exists crawler.tmp_table_email_gen_{}'.format(table_name_id))
    con.commit()
    query = " create table crawler.tmp_table_email_gen_{tab_name_id} as "\
            " (select distinct on (company_linkedin_url,people_linkedin_url) "\
            " list_id,list_items_url_id,company_linkedin_url,people_linkedin_url,name,location_person,designation "\
            " from "\
            "(select  list_id,list_items_url_id, "\
        " trim(unnest(crawler.clean_linkedin_url_array(crawler.extract_related_info(string_to_array(experience,'|'),1)))) company_linkedin_url, "\
        "linkedin_url as people_linkedin_url,name ,location as location_person "\
        " ,trim(unnest(crawler.clean_linkedin_url_array(crawler.extract_related_info(string_to_array(experience,'|'),2)))) designation "\
        " ,trim(unnest(crawler.clean_linkedin_url_array(crawler.extract_related_info(string_to_array(experience,'|'),4)))) work_time "\
                " from {ppl_base_tab} a where "\
        "experience like '%%{{}}%%' and a.list_id = %s  )x "\
                " where work_time like '%%Present%%' and designation ~* %s )"\
        " union "\
        " (select distinct on (company_linkedin_url,linkedin_url)" \
            " list_id,list_items_url_id, "\
        " company_linkedin_url, "\
        "linkedin_url as people_linkedin_url,name,location as location_person,sub_text as designation "\
            " from {ppl_base_tab} a where "\
        "company_linkedin_url like '%%linkedin%%'  "\
        " and array_length(string_to_array(company_linkedin_url,'|'),1) = 1 "\
            " and a.list_id = %s and a.sub_text ~* %s) "\
        " ".format(tab_name_id=table_name_id,ppl_base_tab=people_base_table)
        # and experience not like '%%{{}}%%' removed in the above query
        # tried to find for cases where company length is >1, but experience is '', all cases are errors in data
        # " union "\
        # " (select distinct on (company_linkedin_url,people_linkedin_url) "\
        #     " list_id,list_items_url_id,company_linkedin_url,people_linkedin_url,name,sub_text,location_person,designation "\
        #     " from "\
        # "( select list_id,list_items_url_id, "\
        # " unnest(crawler.clean_linkedin_url_array(string_to_array(company_linkedin_url,'|'))) company_linkedin_url, "\
        # "linkedin_url as people_linkedin_url,name,sub_text as designation,location as location_person "\
        # " ,sub_text as designation "\
        #     " from crawler.linkedin_people_base a where "\
        # "company_linkedin_url like '%%linkedin%%'  and experience not like '%%{{}}%%' "\
        # " and array_length(string_to_array(company_linkedin_url,'|'),1) > 1 "\
        #     " and a.list_id = %s and a.sub_text ~* %s )a "\
        # " where  )"\
        # " ".format(table_name_id)
    con.cursor.execute(query,(list_id,desig_list_reg,list_id,desig_list_reg,))
    # to get domain, we need linkedin url
    query = "delete from crawler.tmp_table_email_gen_{} where company_linkedin_url is null or "\
            " company_linkedin_url = '' ".format(table_name_id)
    con.cursor.execute(query)
    con.commit()

    #################################################################################################################
    ###################### Do not remove below comments w/o checking the codes in it ################################
    #################################################################################################################
    # people in the related people field who are in same company and have valid designation
    # Problem is that linkedin_url of company not available in related_people field. Need to do some string matching
    # company name also is not avaiable directly.need to extract it from designation.
    # this is taking lot of time to execute. need to optimize queries.tmp_table3_1_email_gen table
    # is having >2 million rows in some cases. not using this for now. can use this in cases where number is low
    # con.cursor.execute('drop table if exists crawler.tmp_table3_1_email_gen{}'.format(table_name_id))
    # con.commit()
    # # expand companies first (idea is to get company names and urls and match it with them in related people
    # query = " create table crawler.tmp_table3_1_email_gen{} as select "\
    #     " list_id,list_items_url_id, "\
    #     " trim(unnest(crawler.clean_linkedin_url_array(crawler.extract_related_info(string_to_array(experience,'|'),1)))) company_linkedin_url, "\
    #     " trim(unnest(crawler.clean_linkedin_url_array(crawler.extract_related_info(string_to_array(experience,'|'),3)))) company_name,"\
    #     "related_people from crawler.linkedin_people_base "\
    #     " where experience like '%%{{}}%%' and list_id = %s and " \
    #     " related_people is not null and related_people != '' ".format(table_name_id)
    # con.cursor.execute(query,(list_id,))
    # # delete rows with missing company linkedin url (done to improve performance)
    # query = " delete from crawler.tmp_table3_1_email_gen{} " \
    #         " where company_linkedin_url is null or company_linkedin_url = '' or " \
    #         " company_linkedin_url not like '%linkedin.com/compan%' or " \
    #         " company_name = '' or company_name is null ".format(table_name_id)
    # con.cursor.execute(query)
    # # next expand related_people field
    # con.cursor.execute('drop table if exists crawler.tmp_table3_2_email_gen{}'.format(table_name_id))
    # con.commit()
    # query = " create table crawler.tmp_table3_2_email_gen{table_id} as select distinct "\
    #     " list_id,list_items_url_id, "\
    #     " company_linkedin_url,company_name, "\
    #     " trim(unnest(crawler.extract_related_info(string_to_array(related_people,'|'),1))) as  people_linkedin_url, "\
    #     " trim(unnest(crawler.extract_related_info(string_to_array(related_people,'|'),2))) as  name, "\
    #     " trim(split_part(trim(unnest(crawler.extract_related_info(string_to_array(related_people,'|'),3))),'at ',2)) company_name1, "\
    #     " trim(split_part(trim(unnest(crawler.extract_related_info(string_to_array(related_people,'|'),3))),'at ',1)) designation "\
    #     " from crawler.tmp_table3_1_email_gen{table_id} where "\
    #         " company_name not in ('','NULL') and company_linkedin_url != '' ".format(table_id = table_name_id)
    # con.cursor.execute(query)
    # con.commit()
    # # delete items where names not matching or designation not matching
    # query = " delete from crawler.tmp_table3_2_email_gen{table_id} where "\
    #     " company_name1 != company_name or designation !~* %s or "\
    #         " designation not ilike '%%' || company_name || '%%' ".format(table_id=table_name_id)
    # con.cursor.execute(query,(desig_list_reg,))
    # con.commit()

    # below codes used in the next union queries
    # " union "\
    #         " ( select distinct d.name ,d.designation,b.website,b.company_name,"\
    #         " b.linkedin_url as company_linkedin_url,d.people_linkedin_url,NULL as location_person, "\
    #         " b.headquarters,b.industry,b.company_size,b.founded, "\
    #         " d.list_id,d.list_items_url_id from "\
    #         " crawler.tmp_table3_2_email_gen{table_id} d "\
    #         "join crawler.linkedin_company_redirect_url e on ( d.company_linkedin_url = e.url) "\
    #         " join crawler.linkedin_company_base b on e.redirect_url = b.linkedin_url "\
    #         " ) "\

    #################################################################################################################
    ###################### Do not remove above comments w/o checking the codes in it ################################
    #################################################################################################################

    # people_urls from  company_base
    con.cursor.execute('drop table if exists crawler.tmp_table2_email_gen_{}'.format(table_name_id))
    con.commit()
    query = " create table crawler.tmp_table2_email_gen_{tab_name_id} as select distinct list_id,list_items_url_id, "\
        " linkedin_url as company_linkedin_url, "\
        "unnest(crawler.clean_linkedin_url_array(crawler.extract_related_info(string_to_array(employee_details,'|'),1))) as people_linkedin_url, "\
        " unnest(crawler.clean_linkedin_url_array(crawler.extract_related_info(string_to_array(employee_details,'|'),2))) as name, "\
        " unnest(crawler.clean_linkedin_url_array(crawler.extract_related_info(string_to_array(employee_details,'|'),3))) as designation, "\
        " company_name,website, headquarters,industry,company_size,founded "\
        "from {comp_base_tab} a where "\
        "employee_details like '%%linkedin%%' and a.list_id = %s and a.isvalid=1".format(tab_name_id=table_name_id,
                                                                          comp_base_tab=company_base_table)
    con.cursor.execute(query,(list_id,))
    # reason this query is like this needs to be specified(forgot!!)
    # this was done so that for people whose designation is missing, try to join with people_base table and get details
    query = "delete from crawler.tmp_table2_email_gen_{} where "\
            " (designation is not null and designation != '' "\
            " and (designation !~* %s )) "\
            " ".format(table_name_id)
    # or designation not ilike '%%' || company_name || '%%' # this is not needed as we are looking at employees
    con.cursor.execute(query,(desig_list_reg,))
    con.commit()

    # creating index
    query = "create index on crawler.tmp_table_email_gen_{} (company_linkedin_url)".format(table_name_id)
    con.cursor.execute(query)
    query = "create index on crawler.tmp_table_email_gen_{} (people_linkedin_url)".format(table_name_id)
    con.cursor.execute(query)
    query = "create index on crawler.tmp_table2_email_gen_{} (company_linkedin_url)".format(table_name_id)
    con.cursor.execute(query)
    query = "create index on crawler.tmp_table2_email_gen_{} (people_linkedin_url)".format(table_name_id)
    con.cursor.execute(query)

    # joining both
    con.cursor.execute('drop table if exists crawler.tmp_table1_email_gen_{}'.format(table_name_id))
    con.commit()
    # here union of two sub queries are taken. This is done to improve performance
    # Earlier this was done using single query (using or in the join conditions). This took long time to execute
    query = "create table crawler.tmp_table1_email_gen_{table_id} as "\
            " (select distinct d.name,d.designation,b.website,b.company_name, "\
            " d.company_linkedin_url,d.people_linkedin_url, d.location_person," \
            " b.headquarters,b.industry,b.company_size,b.founded, "\
            " d.list_id,d.list_items_url_id from "\
            "  crawler.tmp_table_email_gen_{table_id} d "\
            " left join crawler.linkedin_company_redirect_url e on ( d.company_linkedin_url = e.url) "\
            " left join {comp_base_tab} b on (e.redirect_url = b.linkedin_url or d.company_linkedin_url=b.linkedin_url) "\
            " where e.url is not null or b.linkedin_url is not null and b.isvalid=1"\
            "  ) "\
            " union "\
            " (select distinct a.name,a.designation,a.website,a.company_name, "\
            " company_linkedin_url,people_linkedin_url,NULL as location_person, "\
            " headquarters,industry,company_size,founded, "\
            " a.list_id,a.list_items_url_id "\
            " from crawler.tmp_table2_email_gen_{table_id} a where designation != '' and designation is not null  "\
            " )"\
            " union "\
            " (select distinct a.name,a.sub_text as designation,b.website,b.company_name, "\
            " d.company_linkedin_url,c.url as people_linkedin_url,a.location as location_person, "\
            " b.headquarters,b.industry,b.company_size,b.founded,"\
            " d.list_id,d.list_items_url_id from "\
            " {ppl_base_tab} a join crawler.linkedin_people_redirect_url c on a.linkedin_url = c.redirect_url "\
            " join crawler.tmp_table2_email_gen_{table_id} d on (c.url=d.people_linkedin_url ) "\
            " join {comp_base_tab} b on d.company_linkedin_url = b.linkedin_url "\
            " where a.sub_text ~* '{regex}'and b.isvalid=1 and (d.designation = '' or d.designation is null ) "\
            " and a.sub_text ilike '%'||b.company_name||'%' ) "\
            " ".format(table_id=table_name_id,regex=desig_list_reg,comp_base_tab=company_base_table,
                       ppl_base_tab=people_base_table)
    # query = "create table crawler.tmp_table1_email_gen_{} as "\
    #         "select a.name,a.sub_text,b.website,b.company_name,d.list_id,d.list_items_url_id from "\
    #         " crawler.linkedin_people_base a  "\
    #         "join crawler.tmp_table_email_gen_{} d on a.linkedin_url=d.people_linkedin_url "\
    #         " join crawler.linkedin_company_base b on d.company_linkedin_url = b.linkedin_url "\
    #         " where d.list_id = %s and a.sub_text ~* '{}' ".format(table_name_id,table_name_id,desig_list_reg)
    con.cursor.execute(query)
    con.commit()
    con.cursor.execute(" update crawler.tmp_table1_email_gen_{} "\
            " set name = regexp_replace(regexp_replace(name,'[^a-zA-Z0-9.()\- ]',' '),' +',' ') ".format(table_name_id))
    # create first name, middle name and last name
    con.cursor.execute('alter table crawler.tmp_table1_email_gen_{} add column name_cleaned text[], add column domain text'.format(table_name_id))
    con.commit()
    query = "update crawler.tmp_table1_email_gen_{} set name_cleaned = crawler.name_cleaner(name), "\
            "domain = replace(substring(website  from '.*://([^/]*)'),'www.','') ".format(table_name_id)
    con.cursor.execute(query)
    con.commit()
    query = "update crawler.tmp_table1_email_gen_{} set domain = '' where "\
            " (domain like '%google.com%' and company_name != 'Google' )  "\
            " or (domain like 'facebook.com%' and company_name != 'Facebook') "\
            " or (domain like 'linkedin.com%' and company_name != 'LinkedIn') "\
            " or (domain like 'yahoo.com%' and company_name != 'Yahoo') "\
            " or (domain like 'twitter.com%' and company_name != 'Twitter') "\
            " or (domain like 'myspace.com%' and company_name != 'Myspace' and company_name != 'MySpace') "\
            " or (domain like 'yelp.com%' and company_name != 'Yelp') "\
            " or (domain like 'youtube.com%' and company_name != 'YouTube') "\
            " or (domain like 'meetup.com%' and company_name != 'Meetup') "\
            " or (domain like 'angel.co%' and company_name != 'AngelList') "\
            " or (domain like 'vimeo.com%' and company_name != 'Vimeo') "\
            " or (domain like 'instagram.com%' and company_name != 'Instagram') "\
            " or (domain like 'companycheck.co.uk%' and company_name != 'Company Check Ltd') "\
            " or (domain like 'tinyurl.com%' and company_name != 'TinyURL') "\
            " or (domain like '%bit.ly%') "\
            " or (domain ~* 'wikipedia.org|sites.google.com|plus.google.com|goo.gl|itunes.apple.com|underconstruction.com') "\
            " or (domain in ('none','N','n','http','-','TBD','TBA','www','http:','.','None','NA','0','fb.com','x',"\
            "'ow.ly','na.na','na','http;','goo.gl','1','com') ) ".format(table_name_id)
    con.cursor.execute(query)
    con.commit()
    query = "insert into crawler.people_details_for_email_verifier_new "\
            " (list_id,list_items_url_id,full_name,first_name,middle_name,last_name,domain,designation,company_name,"\
            " company_website, headquarters,location_person,industry,company_size,founded,company_linkedin_url,people_linkedin_url)  "\
            "select distinct on (name_cleaned[2],name_cleaned[3],name_cleaned[4],website) "\
            " list_id,list_items_url_id,name as full_name, name_cleaned[2] as first_name, name_cleaned[3] as middle_name, "\
            "name_cleaned[4] as last_name, domain,  trim(designation) as designation,company_name, website as company_website, "\
            " headquarters,location_person,industry,company_size,founded,company_linkedin_url,people_linkedin_url "\
            " from crawler.tmp_table1_email_gen_{} "\
            " where name != 'LinkedIn Member' "\
            " on conflict do nothing".format(table_name_id)
    #" where "\
    #        "domain not in ('http://','http://-','http://.','http://...','http://1','') and  "\
    #        "name_cleaned[2] is not null and name_cleaned[4] is not null and name_cleaned[2] not in('','.','..') "\
    #        " and name_cleaned[4] not in ('','.','..') and domain is not null and domain != 'NULL' "\
    #        "and list_id =%s "\
            
    con.cursor.execute(query)
    con.commit()
    # drop tables
    con.cursor.execute('drop table crawler.tmp_table1_email_gen_{}'.format(table_name_id))
    con.cursor.execute('drop table crawler.tmp_table_email_gen_{}'.format(table_name_id))
    con.cursor.execute('drop table crawler.tmp_table2_email_gen_{}'.format(table_name_id))
    con.cursor.execute('drop table if exists crawler.tmp_table3_1_email_gen{}'.format(table_name_id))
    con.cursor.execute('drop table if exists crawler.tmp_table3_2_email_gen{}'.format(table_name_id))
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
    optparser.add_option('-C', '--comptab',
                         dest='comp_base_table',
                         help='company base table name',
                         default='crawler.linkedin_company_base')
    optparser.add_option('-b', '--batch_gen',
                         dest='batch_gen',
                         help='do batch gen if 1',
                         default=0,type='int')
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
    comp_base_table = options.comp_base_table
    batch_gen = options.batch_gen
    con = PostgresConnect()
    con.get_cursor()
    if batch_gen:
        con.execute("select id from crawler.list_table where list_name like %s",(list_name+'%',))
        res = con.cursor.fetchall()
        con.close_cursor()
        con.close_connection()
        if not res:
            raise ValueError('the list name given do not have any records')
        for list_id_tup in res:
            list_id = list_id_tup[0]
            gen_people_details(list_id,desig_list,company_base_table=comp_base_table)
    else:
        con.execute("select id from crawler.list_table where list_name = %s",(list_name,))
        res = con.cursor.fetchall()
        con.close_cursor()
        con.close_connection()
        if not res:
            raise ValueError('the list name given do not have any records')
        list_id = res[0][0]
        gen_people_details(list_id,desig_list,company_base_table=comp_base_table)
