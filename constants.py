__author__ = 'joswin'
import re
# firefox binary location
firefox_binary_loc = 'firefox_binaries/firefox-46/firefox'

# columns in csvs
website_column = 'website'
company_linkedin_col = 'company_linkedin_url'
id_col = 'id'
search_text_column = 'text'
search_text_weight_column = 'weight'

url_validation_reg = re.compile(
        r'^(?:http|ftp)s?://' # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
        r'localhost|' #localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?' # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)

linkedin_dets = [('joswinkj@gmail.com','joswinkj'),('sujay.seetharaman@gmail.com','qatar2022**'),
                ('raditya.mit@gmail.com','pipecandy@1989'),('nikilav.v@gmail.com','naithrav') ]

database='crawler_service_test'
user='postgres'
password='postgres'
host='192.168.3.8'


