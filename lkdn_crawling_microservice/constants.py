__author__ = 'joswin'
import re
#postgres connection details
database='crawler_service_test'
user='postgres'
password='postgres'
host='localhost'

# column names in the csv file
company_name_field = 'name'
company_details_field = 'details'
designations_column_name = 'designations'
linkedin_url_column = 'linkedin_url'

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
              'Senior Product Manager Web', 'VP Engineering', 'VP Marketing', 'VP Mobile', 'VP Product', 'VP Technology']

# desig_list_regex ='\y' + '\y|\y'.join(desig_list) + '\y'
desig_list_regex = '\yDirector HR\y|\ySenior Director\y|\ySenior VP\y|\yVice President\y|\yHead\y|\yGlobal\y|\yChief\y|\yPresident\y|\yVP\y|\yAVP\y|\yEVP\y'

# con_string = 'postgresql://postgres:postgres@localhost:5432/builtwith_data'
problematic_urls_file = 'prob_files.txt'

common_company_wrds = ['limited', 'all', 'just', 'being', 'over', 'both', 'through', 'solutions', 'pty', 'yourselves',
                       'llc', 'its', 'before', 'partners', 'group', 'with', 'had', 'should', 'to', 'only', 'systems',
                       'under', 'ours', 'has', 'do', 'them', 'his', 'very', 'de', 'they', 'new', 'not', 'during',
                       'now', 'him', 'nor', 'school', 'did', 'these', 'each', 'where', 'because', 'pvt', 'doing',
                       'theirs', 'some', 'global', 'gmbh', 'design', 'are', 'our', 'ourselves', 'out', 'what',
                       'network', 'for', 'capital', 'below', 'creative', 'does', 'health', 'above', 'between',
                       'international', 'she', 'be', 'we', 'after', 'business', 'web', 'marketing', 'here', 'corp',
                       'hers', 'by', 'on', 'about', 'of', 'against', 'ltd', 'com', 'or', 'software', 'consulting',
                       'own', 'co', 'into', 'association', 'private', 'yourself', 'down', 'your', 'management', 'from',
                       'her', 'whom', 'there', 'been', 'few', 'too', 'communications', 'themselves', 'was', 'until',
                       'more', 'himself', 'that', 'company', 'but', 'off', 'technologies', 'herself', 'than', 'those',
                       'he', 'me', 'myself', 'this', 'up', 'will', 'while', 'associates', 'can', 'were', 'my', 'and',
                       'then', 'is', 'in', 'am', 'it', 'an', 'as', 'itself', 'at', 'have', 'further', 'technology',
                       'their', 'if', 'again', 'no', 'media', 'agency', 'when', 'same', 'any', 'how', 'other', 'which',
                       'digital',  'inc', 'development', 'who', 'most', 'services', 'such', 'why', 'engineering',
                        'center', 'medical', 'having', 'so', 'corporation', 'the', 'yours', 'once','nan','NaN']

company_common_reg = re.compile(r'\b'+r'\b|\b'.join(common_company_wrds)+r'\b')

#these table names are set based on the crawling_micro_service code. Do not change these before changing it there also.
urls_to_crawl_table = 'linkedin_company_urls_to_crawl_priority'
initial_urls_table = 'linkedin_company_urls_to_crawl_initial_list'
#end : these table names are set based on the crawling_micro_service code. Do not change these before changing it there also
