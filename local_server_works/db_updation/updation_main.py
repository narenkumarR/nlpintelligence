#update table names in the lists below and the timestamp_col field in the function calls appropriately and run this file to update the master tables
from table_updation_new import LinkedinDumpUtil

dump_util = LinkedinDumpUtil()

company_tables_list = ['linkedin_company_base_process_2016_09_08_14_00']
#people_tables_list = ['linkedin_people_base_2016_05_30', 'linkedin_people_base_2016_06_01', 'linkedin_people_base_2016_06_06', 'linkedin_people_base_2016_06_08', 'linkedin_people_base_2016_06_10', 'linkedin_people_base_2016_06_10_1', 'linkedin_people_base_2016_06_10_2', 'linkedin_people_base_2016_06_10_3', 'linkedin_people_base_2016_06_11', 'linkedin_people_base_2016_06_13', 'linkedin_people_base_2016_06_13_1', 'linkedin_people_base_2016_06_15', 'linkedin_people_base_2016_06_16', 'linkedin_people_base_2016_06_20', 'linkedin_people_base_2016_06_22', 'linkedin_people_base_2016_07_15', 'linkedin_people_base_for_investor_saas','linkedin_people_base_2016_05_25','linkedin_people_base_2016_05_27','linkedin_people_base_2016_06_07']
people_tables_list = ['linkedin_people_base_process_2016_09_08_14_00']

#company_array_tables_list = ['linkedin_company_base_2016_06_13_1','linkedin_company_base_2016_05_25']
company_array_tables_list = []
#people_array_tables_list = [ 'linkedin_people_base_for_investor']
people_array_tables_list = []

for t_name in company_tables_list:
    print t_name
    #dump_util.process_company_dump(t_name,array_present=False,timestamp_col='timestamp')
    dump_util.process_company_dump(t_name,array_present=False,timestamp_col='created_on')
    #dump_util.update_company_mapper(t_name)

for t_name in people_tables_list:
    print t_name
    #dump_util.process_people_dump(t_name,array_present=False,timestamp_col='timestamp')
    dump_util.process_people_dump(t_name,array_present=False,timestamp_col='created_on')

for t_name in company_array_tables_list:
    print t_name
    dump_util.process_company_dump(t_name,array_present=True,timestamp_col='timestamp')
    #dump_util.update_company_mapper(t_name)

for t_name in people_array_tables_list:
    print t_name
    dump_util.process_people_dump(t_name,array_present=True,timestamp_col='timestamp')

dump_util.con.close()

