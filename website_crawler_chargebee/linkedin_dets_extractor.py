__author__ = 'joswin'

import logging
import re
from itertools import izip
from linkedin_company_crawler import LinkedinOrganizationService
from proxy_generator import ProxyGen
from postgres_connect import PostgresConnect
from constants import database,host,password,user

class CmpnyLinkedinExtractor(object):
    '''
    '''
    def __init__(self,proxy=True,login=False,visible=False):
        '''
        :param proxy:
        :param login:
        :param visible:
        :return:
        '''
        self.proxy = proxy
        self.table_fields = ['Linkedin URL','Company Name','Company Size','Industry','Type','Headquarters',
                                 'Description Text','Founded','Specialties','Website'
                ,'Employee Details','Also Viewed Companies']
        self.table_field_names = ['linkedin_url','company_name','company_size','industry','company_type','headquarters',
                                  'description','founded','specialties','website','employee_details_array',
                                  'also_viewed_companies_array']
        self.query = "select {} from linkedin_company_base where linkedin_url = %s limit 1".format(
            ','.join(self.table_field_names))
        self.start(proxy=proxy,login=login,visible=visible)

    def start(self,proxy=True,login=False,visible=False):
        '''
        :param proxy:
        :param login:
        :param visible:
        :return:
        '''
        if self.proxy and not login:
            self.proxies = []
            self.ip_matcher = re.compile("^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")
            self.proxy_generator = ProxyGen(visible=visible)
            proxy_ip,proxy_port = self.get_proxy()
        else:
            self.proxy = False
            proxy_ip,proxy_port = None,None
        self.linkedin_scraper = LinkedinOrganizationService(visible=visible,login=login,proxy=self.proxy,
                                                            proxy_ip=proxy_ip,proxy_port=proxy_port)
        self.con = PostgresConnect()
        self.con.get_cursor()

    def exit(self):
        '''
        :return:
        '''
        self.linkedin_scraper.exit()
        self.con.close_cursor()
        self.con.close_connection()

    def gen_proxies(self):
        '''
        :return:
        '''
        logging.info('company part: trying to get proxies')
        if not self.proxy:
            return
        else:
            try:
                self.proxy_generator.activate_browser()
                proxies = self.proxy_generator.generate_proxy()
            except Exception :
                logging.exception('company part: could not create proxies. using None')
                proxies = [(None,None)]
            try:
                self.proxy_generator.exit()
            except:
                pass
        logging.info('company part: Proxies fetched {}'.format(proxies))
        for i in proxies:
            if i[0] is not None:
                if self.ip_matcher.match(i[0]):
                    self.proxies.append(i)
        # logging.info('All Proxies fetched {}'.format(self.proxies)) #not printing proxy list. only object name

    def get_proxy(self):
        ''' call this when a proxy is needed
        :return:
        '''
        if not self.proxy:
            return (None,None)
        if not self.proxies:
            self.gen_proxies()
            if not self.proxies:
                return (None,None)
        return self.proxies.pop()

    def get_from_db(self,url):
        '''
        :param url:
        :return:
        '''
        self.con.execute(self.query,(url,))
        dets = self.con.cursor.fetchall()
        if not dets:
            return {}
        else:
            dets = dets[0]
            dets_dic = {}
            for field,value in izip(self.table_fields,dets):
                dets_dic[field] = value
            return dets_dic


    def get_linkedin_details(self,url,look_for='Company Name'):
        '''
        :param url:
        :return:
        '''
        res_from_db = self.get_from_db(url)
        if res_from_db and res_from_db.get(look_for,''):
            return res_from_db
        else:
            return self.linkedin_scraper.get_organization_details_from_linkedin_link(url)