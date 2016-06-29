#pkill -9 firefox
pkill -9 Xvfb
find /tmp/* -maxdepth 1 -type d -name 'tmp*' |  xargs rm -rf
python crawler_generic.py 107 2 False linkedin_people_urls_to_crawl_for_investor linkedin_people_urls_to_crawl_for_investor_priority linkedin_people_base_for_investor linkedin_company_urls_to_crawl_for_investor linkedin_company_urls_to_crawl_for_investor_priority linkedin_company_base_for_investor linkedin_company_finished_urls_for_investor linkedin_people_finished_urls_for_investor
find /tmp/* -maxdepth 1 -type d -name 'tmp*' |  xargs rm -rf
