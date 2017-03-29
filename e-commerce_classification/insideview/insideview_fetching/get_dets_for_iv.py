
import pandas as pd
from sqlalchemy import create_engine

database='insideview_app_db'
user='postgres'
password='postgres'
host='192.168.3.57'

def get_comp_dets_for_insideview_fetch(out_loc = 'input_company_details.csv',no_of_comps = 100):
    engine = create_engine('postgresql://{user_name}:{password}@{host}:{port}/{database}'.format(
            user_name=user,password=password,host=host,port='5432',database=database
        ))
    with engine.connect() as con:
        query = " update public.companies_for_insideview_fetch a set insideview_search_status = 'pending' "\
                    " from (select id from public.companies_for_insideview_fetch"\
                    " where insideview_search_status='not_done' order by random() limit {})b "\
                    " where a.id=b.id ".format(no_of_comps)
        con.execute(query)
    query = " select company_name,website,city,state,country from public.companies_for_insideview_fetch " \
            " where insideview_search_status = 'pending'  "
    df = pd.read_sql_query(query,engine)
    df = df.fillna('')
    df.to_csv(out_loc,index=False,quoting=1,encoding='utf-8')
    with engine.connect() as con:
        con.execute("update public.companies_for_insideview_fetch set insideview_search_status='started'"
                    " where insideview_search_status = 'pending' ")
    engine.dispose()
