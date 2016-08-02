__author__ = 'joswin'


# generating websites for colorado companies and putting them into crawling table
import postgres_connect
from api_analysis import CrunchBaseExtra

database1,user1,password1,host1='linkedin_data','postgres','postgres','localhost'
database2,user2,password2,host2='crawler_service_test','postgres','postgres','localhost'


con1 = postgres_connect.PostgresConnect(database_in=database1,user_in=user1,password_in=password1,host_in=host1)
con2 = postgres_connect.PostgresConnect(database_in=database2,user_in=user2,password_in=password2,host_in=host2)

con1.get_cursor()
con2.get_cursor()
query = "select split_part(cb_url,'organization/',2) org_cb_id,company_name,domain,a.uuid "\
    "from crunchbase_data.organizations a left join crunchbase_data.websites b on a.uuid=b.org_uuid "\
    "where b.uuid is null and country_code = 'USA' and state_code = 'CO'"
con1.execute(query)
res = con1.cursor.fetchall()
cb = CrunchBaseExtra('60816b1a93440ae7f1fa40935141525a')

# first insert all result in res into crawler db
list_id = '3dacd6dc-4ffd-11e6-ad3a-df63592beb60'
records_list_template = ','.join(['%s']*len(res))
query = "insert into crawler.list_items (list_id,list_input,list_input_additional) values {} "\
        "on conflict do nothing".format(records_list_template)
insert_list = [(list_id,i[2],i[1]) for i in res]
con2.execute(query,insert_list)
con2.commit()
query = "select list_input,list_input_additional,id from crawler.list_items where list_id = %s and "\
    "list_input in %s and list_input_additional in %s"
con2.execute(query,(list_id,tuple([i[2] for i in res]),tuple([i[1] for i in res])))
list_items_list = con2.cursor.fetchall()
list_items_dic = {}
for i in list_items_list:
    list_items_dic[(i[0],i[1])] = i[2] # (list_input(browser), list_input_additional(name)) format

from psycopg2 import ProgrammingError,InternalError

for i in res:
    try:
        print i
        cb_username,name,domain,org_uuid = i[0],i[1],i[2],i[3]
        websites_list = cb.websites_list(cb_username)
        if not websites_list:
            print 'no result'
            continue
        websites_for_insert = [(org_uuid,api_res[0],api_res[1],api_res[2])  for api_res in websites_list ]
        records_list_template = ','.join(['%s']*len(websites_for_insert))
        insert_query = "INSERT INTO {} (org_uuid,url,website_type,uuid) VALUES {} ".format('crunchbase_data.websites',records_list_template)
        con1.execute(insert_query, websites_for_insert)
        con1.commit()
        linkedin_urls = [api_res[0] for api_res in websites_list if api_res[1]=='linkedin']
        if linkedin_urls:
            linkedin_url = linkedin_urls[0]
            query = "insert into crawler.list_items_urls (list_id,list_items_id,url) values (%s,%s,%s) "\
                    "on conflict do nothing"
            con2.execute(query,(list_id,list_items_dic[(domain,name)],linkedin_url))
            con2.commit()
            print 'inserted result'
    except ProgrammingError:
        print 'programming error'
        con1.close_cursor()
        con2.close_cursor()
        con1.get_cursor()
        con2.get_cursor()
    except InternalError:
        print 'internal error'
        con1.close_cursor()
        con2.close_cursor()
        con1.get_cursor()
        con2.get_cursor()
    except:
        print 'some other error'
        continue