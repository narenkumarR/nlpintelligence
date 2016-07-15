--company table
create table linkedin_company_domains as select linkedin_url,replace(substring(website  from '.*://([^/]*)'),'www.','') as domain from linkedin_company_base;
create table tmp_table as select distinct * from linkedin_company_domains;
drop table linkedin_company_domains;
alter table tmp_table rename to linkedin_company_domains;
create unique index on linkedin_company_domains (linkedin_url,domain);
create index on linkedin_company_domains(domain);
create index on linkedin_company_domains(linkedin_url);

--removing linkedin.com/company/ part
alter table linkedin_company_domains add column linkedin_name text;
update linkedin_company_domains set linkedin_name = split_part(linkedin_url,'linkedin.com/company/',2) where  linkedin_url like '%linkedin.com/company/%';
update linkedin_company_domains set linkedin_name = split_part(linkedin_url,'linkedin.com/companies/',2) where  linkedin_url like '%linkedin.com/companies/%';

--invalid urls
select * from linkedin_company_domains where linkedin_name is null and linkedin_url not like '%/edu/%' and 
	linkedin_url not like '%.com/groups%' and linkedin_url not like '%.com/grps%' and linkedin_url not like '%.com/grp%' 
	and linkedin_url not like '%/profile/%' and linkedin_url not like '%/in/%' and linkedin_url not like '%/pub/%' limit 100;


update linkedin_company_domains set domain = '' where domain like '%google.com%' and linkedin_name != 'google';
update   linkedin_company_domains set domain = '' where domain like '%facebook.com%' and linkedin_name !=  'facebook';
update linkedin_company_domains set domain = '' where domain like '%linkedin.com%' and linkedin_name != 'linkedin';
update linkedin_company_domains set domain = '' where domain like '%yahoo.com%' and (linkedin_name != 'yahoo' and linkedin_name != '1288') ;
update linkedin_company_domains set domain = '' where domain like '%twitter.com%' and linkedin_name != 'twitter';
update linkedin_company_domains set domain = '' where domain like '%myspace.com%' and linkedin_name != 'myspace';
update linkedin_company_domains set domain = '' where domain like '%yelp.com%' and linkedin_name != 'Yelp';
update linkedin_company_domains set domain = '' where domain = 'none';
update linkedin_company_domains set domain = '' where domain = 'N';
update linkedin_company_domains set domain = '' where domain in ( 'n','http','-','TBD','TBA');
update linkedin_company_domains set domain = '' where domain in ('www','http:','.','None','NA','sites.google.com','0','fb.com','x','ow.ly');
update linkedin_company_domains set domain = '' where domain like '%youtube.com%' and linkedin_name != 'youtube';
update linkedin_company_domains set domain = '' where domain in ('na.na','na','http;','goo.gl','1','plus.google.com','com','itunes.apple.com','underconstruction.com');
update linkedin_company_domains set domain = '' where domain ='gov.uk';
update linkedin_company_domains set domain = '' where domain like '%meetup.com%' and linkedin_name != 'meetup';
update linkedin_company_domains set domain = '' where domain like '%wikipedia.org%' and linkedin_name != 'wikipedia';
update linkedin_company_domains set domain = '' where domain like '%angel.co%' and linkedin_name != 'angellist';
update linkedin_company_domains set domain = '' where domain like '%vimeo.com%' and linkedin_name != 'vimeo';
update linkedin_company_domains set domain = '' where domain like '%instagram.com%' and linkedin_name != 'instagram';
update linkedin_company_domains set domain = '' where domain like '%companycheck.co.uk%' and linkedin_name != 'company-check-ltd';
update linkedin_company_domains set domain = '' where domain like '%tinyurl.com%' and linkedin_name != 'tinyurl';
