
import psycopg2
import datetime
import logging

def update_locations():
    database='linkedin_data'
    user='postgres'
    password='$P$BptPVyArwpjzWXe1wz1cafxlpmVlGE'
    host='localhost'
    logging.basicConfig(filename='location_updation.log', level=logging.INFO,format='%(asctime)s %(message)s')
    con = psycopg2.connect(database=database, user=user,password=password,host=host)
    cursor = con.cursor()
    logging.info('updating using regions data')
    start_time = datetime.datetime.now()
    logging.info('process started at {}'.format(start_time))
    query = "update public.linkedin_company_domains a set region=b.region,country=b.country from location_tabs.regions_ref_table_regex b "\
         " where (headquarters like region_code_like or headquarters ilike region_like) and "\
        " (headquarters like country_code_like or headquarters ilike country_like) "
    cursor.execute(query)
    con.commit()
    logging.info('completed regions updation. Time difference : {}'.format(datetime.datetime.now()-start_time))
    cursor.close()
    logging.info('updating using country data')
    con = psycopg2.connect(database=database, user=user,password=password,host=host)
    cursor = con.cursor()
    start_time = datetime.datetime.now()
    logging.info('process started at {}'.format(start_time))
    query = "update public.linkedin_company_domains a set location = b.location_cleaned,region=b.region,country=b.country from location_tabs.locations_ref_table_regex b "\
            " where (headquarters like location_code_like or headquarters ilike location_like) and "\
        " (headquarters like region_code_like or headquarters ilike region_like) and "\
        " (headquarters like country_code_like or headquarters ilike country_like)"
    cursor.execute(query)
    con.commit()
    logging.info('completed locations updation. Time difference : {}'.format(datetime.datetime.now()-start_time))
    cursor.close()

if __name__ == '__main__':
    update_locations()