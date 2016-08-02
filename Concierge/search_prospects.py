__author__ = 'joswin'

from itertools import product
from postgres_connect import PostgresConnect

class SearchProspects(object):
    '''
    '''
    def __init__(self):
        self.con = PostgresConnect()

    def search_db(self,key_name,employee_count=None,country=None,state=None,city=None,industry=None,funding_rounds=None,
                  funding_total_usd=None,text_search=None,founded_on=None,technologies=[]):
        '''
        :param key_name: key needed to create table names.(to allow multiple processes at same time)
        :param employee_count: list of size (same form as in linkedin . Eg: ['1-10','11-50']
        :param country:list of countries. Provide codes also (like 'US','USA') for better results
        :param state:list of states. provide codes also
        :param city:list of city names
        :param industry: list of industries
        :param funding_rounds: list of funding rounds
        :param funding_total_usd:tuple(funding_min,funding_max) : between these amounts(including both)
        :param text_search:list of texts. These will be searched in the description fields
        :param founded_on:list of years
        :param technologies:list of technologies. Exact matches will be looked for in the BW data.
        :return:
        '''
        if not key_name:
            raise ValueError('Need to provide valid name')
        lkdn_key,cb_key,bw_key = {},{},{}
        if employee_count:
            lkdn_key['company_size'] = employee_count

        if country and state and :
            lkdn_key['headquarters'] = [for i,j in product(country,state)]
        if state:
            lkdn_key['headquarters'] = lkdn_key.get('headquarters','')+state+'|'
        if industry:
            key_dic['industry'] = industry
        if funding_rounds:
            key_dic['funding_rounds'] = funding_rounds
        if funding_total_usd:
            key_dic['funding_total_usd'] = funding_total_usd
        if text_search:
            key_dic['text_search'] = text_search
        if founded_on:
            key_dic['founded_on'] = founded_on
        if technologies:
            key_dic['technologies'] = technologies
        if not key_dic:
            raise ValueError('Need to provide atleast 1 search criteria')
        table_name = 'concierge_table_{}'.format(key_name)
        self.con.get_cursor()
        self.con.execute('drop table if exists {}'.format(table_name+'_1'))
        self.con.commit()


