drop table if exists public.companies_for_insideview_fetch;
create table public.companies_for_insideview_fetch (
	id serial primary key,
	company_name text,
	website text,
	city text,
	state text,
	country text,
	insideview_search_status text default 'not_done' --pending,not_done,started
);
create unique index on public.companies_for_insideview_fetch (company_name,website,city,state,country);

--insert from insideview search table (first take those with revenue>3)
insert into public.companies_for_insideview_fetch (company_name,city,state,country) select company_name,city,state,country from (select distinct on (a.new_company_id) a.* from crawler.insideview_company_name_filter_search a join crawler.insideview_company_details_filter_search b on a.list_id=b.list_id and a.new_company_id=b.new_company_id where revenue::numeric > 3 )x on conflict do nothing;
