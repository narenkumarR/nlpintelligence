__author__ = 'joswin'

import pandas as pd
from optparse import OptionParser
from postgres_connect import PostgresConnect
from constants import designations_column_name,desig_list_regex

ppl_table_fields = ['input_website','input_company_name','id', 'list_id', 'list_items_url_id', 'full_name',
                    'first_name', 'middle_name', 'last_name',
                    'domain', 'designation', 'company_name', 'company_website', 'headquarters', 'location_person',
                    'industry', 'company_size', 'founded', 'company_linkedin_url', 'people_linkedin_url', 'created_on']

def save_to_file(list_name,desig_loc=None,out_loc='people_details_extracted.csv'):
    '''
    :param list_name:
    :param out_loc:
    :return:
    '''
    con = PostgresConnect()
    query = 'select id from {} where list_name = %s'.format('crawler.list_table')
    con.get_cursor()
    con.cursor.execute(query,(list_name,))
    res_list = con.cursor.fetchall()
    if not res_list:
        raise ValueError('Give correct list name')
    else:
        list_id = res_list[0][0]
    if desig_loc:
        inp_df = pd.read_csv(desig_loc)
        desig_list = list(inp_df[designations_column_name])
    else:
        desig_list = None
    if not desig_list:
        # desig_list_reg = desig_list_regex
        raise ValueError('Need designation list file')
    else:
        desig_list_reg = '\y' + '\y|\y'.join(desig_list) + '\y'
    query = "select distinct on (a.first_name,a.middle_name,a.last_name,a.domain) " \
        " d.list_input as input_website,d.list_input_additional input_company_name, "\
        " a.* from crawler.people_details_for_email_verifier_new a join crawler.linkedin_company_redirect_url b on " \
        " (a.company_linkedin_url = b.redirect_url or a.company_linkedin_url = b.url) " \
        " join crawler.list_items_urls c on (b.redirect_url=c.url or b.url=c.url) " \
        " join crawler.list_items d on c.list_id=d.list_id and c.list_items_id=d.id "\
        " where c.list_id = %s and a.domain is not null and designation ~* %s"
    con.cursor.execute(query,(list_id,desig_list_reg,))
    res_list = con.cursor.fetchall()
    df = pd.DataFrame.from_records(res_list)
    df.columns = ppl_table_fields
    df.to_csv(out_loc,index=False,quoting=1)
    con.close_cursor()
    con.close_connection()

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
    optparser.add_option('-o', '--out_file',
                         dest='out_file',
                         help='location of csv containing people details ',
                         default='people_details_extracted.csv')
    (options, args) = optparser.parse_args()
    list_name = options.list_name
    out_file = options.out_file
    desig_loc = options.desig_loc
    save_to_file(list_name,desig_loc,out_file)