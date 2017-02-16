
import re
from nltk.corpus import stopwords
#postgres connection details
database='crawler_service_test'
user='postgres'
password='postgres'
host='192.168.3.8'

# column names in the csv file
company_name_field = 'name'
company_details_field = 'details'
designations_column_name = 'designations'
linkedin_url_column = 'linkedin_url'

