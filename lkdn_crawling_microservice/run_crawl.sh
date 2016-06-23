#pkill -9 firefox
pkill -9 Xvfb
find /tmp/* -maxdepth 1 -type d -name 'tmp*' |  xargs rm -rf
cd crawling_micro_service/
python crawler_generic.py $1 $2 False linkedin_people_urls_to_crawl linkedin_people_urls_to_crawl_priority linkedin_people_base linkedin_company_urls_to_crawl linkedin_company_urls_to_crawl_priority linkedin_company_base linkedin_company_finished_urls linkedin_people_finished_urls
find /tmp/* -maxdepth 1 -type d -name 'tmp*' |  xargs rm -rf
