
# import re
# from nltk.corpus import stopwords
#postgres connection details
database='insideview_app_db'
user='postgres'
password='postgres'
host='127.0.0.1'

# column names in the csv file
company_name_field = 'name'
company_details_field = 'details'
designations_column_name = 'designations'
linkedin_url_column = 'linkedin_url'

accessToken = 'XDl0o6H4B+FprJ3rG/aloG6fqFwJg+LK4/1Z/uBbAc/s8y1/FbOnLX5wy3pQbRffNcCVMpMOIiJLTY4V1xRQbPHT2b5N44BhHKyv9+8z2pE01mNW0R2ADcc0gFdqcs1ijukze2iSoptp2+yC4GBkjiNsBgQ/Q2NnpE1zL12sRFI=.eyJmZWF0dXJlcyI6Int9IiwiY2xpZW50SWQiOiJndjNhMmI4bGI2dnE3bmhqcDFhYSIsImdyYW50VHlwZSI6ImNyZWQiLCJ0dGwiOiIzMTUzNjAwMCIsImlhdCI6IjE0ODM2OTQxNDkiLCJqdGkiOiIyZGFmNjA1NS00MzI5LTRiNWItYTkyNy1iM2NiZjU1OWJjMWMifQ=='
# accessToken = 'U8nSVwyMAKMn+DTUkewvFNHHQiUpZCoRx+o2c1CQ3ysXXiyOxYK1J+tQf7s3cr4jCmC7zeVvu7Tk9y+3rISSjWtJUWYuUoxBZG0t1gj6BmhTO/cdOSPMRKNwzFru8OoBsyFISpDe3RHJo4JCHzOsnATR7apJbkwUJHuUzBxNRO4=.eyJmZWF0dXJlcyI6Int9IiwiY2xpZW50SWQiOiIxNGJ1NGJhaWoxbWY3cm9lNGJmZyIsImdyYW50VHlwZSI6ImNyZWQiLCJ0dGwiOiIzMTUzNjAwMCIsImlhdCI6IjE0ODQ3MzI3OTYiLCJqdGkiOiIyZWMyNTEzNS0zMTdiLTRiNWUtYWRlNS0zODBhMzdmOWEwNGIifQ=='