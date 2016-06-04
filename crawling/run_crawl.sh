pkill -9 firefox
pkill -9 Xvfb
find /tmp/* -maxdepth 1 -type d -name 'tmp*' |  xargs rm -rf
python crawler_generic.py 4000 2 False linkedin_people_urls_to_crawl_inbout_website linkedin_people_urls_to_crawl_priority linkedin_people_base linkedin_company_urls_to_crawl linkedin_company_urls_to_crawl_priority linkedin_company_base
find /tmp/* -maxdepth 1 -type d -name 'tmp*' |  xargs rm -rf
