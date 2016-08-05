set search_path to prospect_db;
--first populate the location table

-- FIRST INSERT EMPTY ROW. this will indicate location missing
insert into prospect_db.location_hierarchy
	(country,state,city) VALUES
	(NULL,NULL,NULL);

--insert all countries from country table
insert into prospect_db.location_hierarchy
	(country,state,city)
	select distinct country,NULL,NULL from
	location_tabs.countries_ref_table_regex;

--next insert all region names
insert into prospect_db.location_hierarchy
	(country,state,city)
	select distinct country,region,NULL from
	location_tabs.regions_ref_table_regex
	on conflict do nothing;
--LOCATION needs cleaning in the location_tabs tables. eg: "American Samoa (see also separate entry under AS)": this needs to be cleaned

