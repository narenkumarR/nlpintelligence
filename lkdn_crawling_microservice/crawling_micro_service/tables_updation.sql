
--insert unfinished urls in the initial list to priority table
insert into linkedin_company_urls_to_crawl_priority
    select a.url from linkedin_company_urls_to_crawl_initial_list a left join
    linkedin_company_finished_urls b on a.url=b.url where b.url is null;

--insert also viewed companies in the initial list to priority table
drop table if exists tmp_table;

create table tmp_table as
    select unnest(clean_linkedin_url_array(extract_related_info(string_to_array(a.also_viewed_companies,'|'),1))) as url
    from linkedin_company_base a join linkedin_company_urls_to_crawl_initial_list b on a.linkedin_url = b.url
    where  also_viewed_companies like '%linkedin%' ;

insert into linkedin_company_urls_to_crawl_priority
    select a.url from tmp_table a left join linkedin_company_finished_urls b on a.url = b.url
    where b.url is null on conflict do nothing;

--insert employee details to priority table
--drop table if exists tmp_table;
--
--create table tmp_table as
--select unnest(clean_linkedin_url_array(extract_related_info(string_to_array(employee_details,'|'),1))) as url,
--    unnest(clean_linkedin_url_array(extract_related_info(string_to_array(employee_details,'|'),3))) as position
--    from linkedin_company_base where employee_details like '%linkedin%' ;
--
--insert into linkedin_people_urls_to_crawl_priority
-- select a.url from tmp_table a left join
--    linkedin_people_finished_urls b on a.url=b.url
--    where b.url is null on conflict do nothing;


delete from linkedin_people_urls_to_crawl_priority where url not like '%linkedin%' or url like '%,%' or url like '%|%' or url like '%{}%';
delete from linkedin_company_urls_to_crawl_priority where url not like '%linkedin%' or url like '%,%' or url like '%|%' or url like '%{}%';
