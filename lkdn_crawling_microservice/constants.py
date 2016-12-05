__author__ = 'joswin'
import re
from nltk.corpus import stopwords
#postgres connection details
database='crawler_service_test'
user='postgres'
password='postgres'
host='localhost'

# aws server credentials (this is done to take existing results from prospect table)
prospect_database='pipecandy_db1'
prospect_user='pipecandy_user'
prospect_password='pipecandy'
prospect_host='192.168.3.6'
# prospect_database='crawler_service_test'
# prospect_user='postgres'
# prospect_password='postgres'
# prospect_host='localhost'

# column names in the csv file
company_name_field = 'name'
company_details_field = 'details'
designations_column_name = 'designations'
linkedin_url_column = 'linkedin_url'

# linkedin credentials
linkedin_username = 'joswinkj@gmail.com'
linkedin_password = 'joswinkj'
linkedin_dets = [('joswinkj@gmail.com','joswinkj'),('sujay.seetharaman@gmail.com','qatar2022**'),
                ('raditya.mit@gmail.com','pipecandy@1989') ]

# firefox binary location
firefox_binary_loc = 'firefox_binaries/firefox-46/firefox'

# table names
company_urls_to_crawl_table = 'crawler.linkedin_company_urls_to_crawl_priority'
company_urls_to_crawl_priority_table = 'crawler.linkedin_company_urls_to_crawl_priority'
company_finished_urls_table = 'crawler.linkedin_company_finished_urls'

#details of the table with people details for email extraction
people_detail_table = 'crawler.people_details_for_email_verifier'
people_detail_table_cols = ['id', 'list_id', 'list_items_url_id', 'first_name', 'middle_name', 'last_name', 'domain', 'designation', 'company_name', 'company_website', 'created_on']
people_detail_table_cols_str = ','.join(people_detail_table_cols)

# designations
desig_list = ['AVP Engineering', 'AVP Marketing', 'AVP Mobile', 'AVP Product', 'AVP Technology',
              'Associate Product Manager', 'Associate Product Manager Mobile', 'Associate Product Manager Web',
              'CEO', 'CIO', 'CMO', 'CTO', 'Chief Executive Officer', 'Chief Information Officer',
              'Chief Marketing Officer', 'Chief Product Officer', 'Chief Technology Officer', 'Co Founder',
              'Co-founder', 'CoFounder', 'Director Engineering', 'Director Mobile', 'Director Technology',
              'Founder', 'Head of Mobility', 'Head of Product', 'Managing Director Engineering',
              'Managing Director Mobile', 'Managing Director Technology', 'Product Manager',
              'Product Manager Mobile', 'Product Manager Web', 'SVP Engineering', 'SVP Marketing', 'SVP Mobile',
              'SVP Product', 'SVP Technology', 'Senior Director Engineering', 'Senior Director Mobile',
              'Senior Director Technology', 'Senior Product Manager', 'Senior Product Manager Mobile',
              'Senior Product Manager Web', 'VP Engineering', 'VP Marketing', 'VP Mobile', 'VP Product', 'VP Technology',
              'VP','AVP','EVP','Head','President','Chief','Global']

desig_list_regex ='\y' + '\y|\y'.join(desig_list) + '\y'
# desig_list_regex = 'Director.+HR|Director.+Human Resources|Director.+Human Resource|Director.+Learning|Director.+Skill development|Director.+Training|Director.+Talent Management|VP.+HR|VP.+Human Resources|VP.+Human Resource|VP.+Learning|VP.+Skill development|VP.+Training|VP.+Talent Management|AVP.+HR|AVP.+Human Resources|AVP.+Human Resource|AVP.+Learning|AVP.+Skill development|AVP.+Training|AVP.+Talent Management|EVP.+HR|EVP.+Human Resources|EVP.+Human Resource|EVP.+Learning|EVP.+Skill development|EVP.+Training|EVP.+Talent Management|Head.+HR|Head.+Human Resources|Head.+Human Resource|Head.+Learning|Head.+Skill development|Head.+Training|Head.+Talent Management|Global.+HR|Global.+Human Resources|Global.+Human Resource|Global.+Learning|Global.+Skill development|Global.+Training|Global.+Talent Management|Chief.+HR|Chief.+Human Resources|Chief.+Human Resource|Chief.+Learning|Chief.+Skill development|Chief.+Training|Chief.+Talent Management|President.+HR|President.+Human Resources|President.+Human Resource|President.+Learning|President.+Skill development|President.+Training|President.+Talent Management'

# con_string = 'postgresql://postgres:postgres@localhost:5432/builtwith_data'
problematic_urls_file = 'prob_files.txt'

# this is for constructing regular expression.
nltk_stops = stopwords.words()
wrds_to_remove = ['pty', 'llc', 'pvt(\.)?','private', 'corp(\.)?','corporation',
                       'ltd(\.)?', 'limited','co(\.)?',
                         'inc(\.)?'
                       ]
common_company_wrds = list(set(nltk_stops+wrds_to_remove))
company_common_reg = re.compile(r'\b'+r'\b|\b'.join(common_company_wrds)+r'\b',re.IGNORECASE)

#these table names are set based on the crawling_micro_service code. Do not change these before changing it there also.
urls_to_crawl_table = 'linkedin_company_urls_to_crawl_priority'
initial_urls_table = 'linkedin_company_urls_to_crawl_initial_list'
#end : these table names are set based on the crawling_micro_service code. Do not change these before changing it there also

user_agents = ['Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)',
               "Mozilla/5.0 (compatible; MSIE 10.0; Macintosh; Intel Mac OS X 10_7_3; Trident/6.0)'",
               "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0; GTB7.4; InfoPath.2; SV1; .NET CLR 3.3.69573; WOW64; en-US)",
               "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.2 (KHTML, like Gecko) Chrome/22.0.1216.0 Safari/537.2'",
               "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.13 (KHTML, like Gecko) Chrome/24.0.1290.1 Safari/537.13",
               "Mozilla/5.0 (X11; CrOS i686 2268.111.0) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.57 Safari/536.11",
               "Mozilla/5.0 (Windows NT 6.2; Win64; x64; rv:16.0.1) Gecko/20121011 Firefox/16.0.1",
               "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:15.0) Gecko/20100101 Firefox/15.0.1",
               "Mozilla/5.0 (iPad; CPU OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10A5355d Safari/8536.25",
               "Mozilla/5.0 (X11; Linux x86_64; rv:41.0) Gecko/20100101 Firefox/41.0"]
# user_agents = ['Mozilla/5.0 (Linux; Android 5.1; Archos Diamond S Build/LMY47D; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/51.0.2704.81 Mobile Safari/537.36']