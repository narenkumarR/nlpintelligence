__author__ = 'joswin'

from postgres_connect import PostgresConnect

def gen_people_details(list_id):
    con = PostgresConnect()
    con.get_cursor()
    # all people urls and current company
    con.cursor.execute('drop table if exists crawler.tmp_table')
    con.commit()
    query = " create table crawler.tmp_table as select list_id,list_items_url_id, "\
        " unnest(crawler.clean_linkedin_url_array(string_to_array(company_linkedin_url,'|'))) company_linkedin_url, "\
        "linkedin_url as people_linkedin_url from crawler.linkedin_people_base a where "\
        "company_linkedin_url like '%%linkedin%%' and a.list_id = %s "
    con.cursor.execute(query,(list_id,))
    con.commit()
    # people_urls from current company
    con.cursor.execute('drop table if exists crawler.tmp_table2')
    con.commit()
    query = " insert into crawler.tmp_table select list_id,list_items_url_id, "\
        " linkedin_url as company_linkedin_url, "\
        "unnest(crawler.clean_linkedin_url_array(crawler.extract_related_info(string_to_array(employee_details,'|'),1))) as people_linkedin_url "\
        "from crawler.linkedin_company_base a where "\
        "employee_details like '%%linkedin%%' and a.list_id = %s "
    con.cursor.execute(query,(list_id,))
    con.commit()
    # joining both
    con.cursor.execute('drop table if exists crawler.tmp_table1')
    con.commit()
    query = "create table crawler.tmp_table1 as "\
            "select a.name,a.sub_text,b.website,d.list_id,d.list_items_url_id from "\
            " linkedin_people_base a join linkedin_people_redirect_url c on a.linkedin_url = c.redirect_url "\
            "join tmp_table d on (c.redirect_url=d.people_linkedin_url or c.url = d.people_linkedin_url) "\
            "join linkedin_company_redirect_url e on (d.company_linkedin_url = e.url or d.company_linkedin_url = e.redirect_url) "\
            " join linkedin_company_base b on e.redirect_url = b.linkedin_url where d.list_id = %s "
    con.cursor.execute(query,(list_id))
    con.commit()
    # create first name, middle name and last name
    con.cursor.execute('alter table crawler.tmp_table1 add column name_cleaned text[], add column domain text')
    con.commit()
    query = "update crawler.tmp_table1 set name_array = name_cleaner(name), "\
            "domain = replace(substring(website  from '.*://([^/]*)'),'www.','') "
    con.cursor.execute(query)
    con.commit()
    query = "insert into crawler.people_details_for_email_verifier (list_id,list_items_url_id,first_name,middle_name,last_name,domain) values "\
            "select distinct list_id,list_items_url_id, name_cleaned[2] as first_name, name_cleaned[3] as middle_name, "\
            "name_cleaned[4] as last_name, domain from crawler.tmp_table1 where "\
            "domain not in ('http://','http://-','http://.','http://...','http://1','') and  "\
            "name_cleaned[2] is not null and name_cleaned[4] is not null and name_cleaned[2] not in('','.','..') "\
            " and name_cleaned[4] not in ('','.','..') and domain is not null and domain != 'NULL' "\
            "and list_id =%s "\
            " on conflict do nothing"
    con.cursor.execute(query,(list_id,))
    con.commit()
    # drop tables
    # con.cursor.execute('drop table crawler.tmp_table1')

