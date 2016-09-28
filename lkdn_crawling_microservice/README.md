
Before running the code for the first time, create schema and tables in postgres using 'postgres_setup.sql'.
For selenium to work, make sure the code is pointing to the correct version of firefox binary (if using binary).
Selenium can have issues with latest versions of firefox. Use firefox 46 binary in that case.


Running the code:
    python main.py -n "list_name" -f "csv_file" -d "designations_file" #example files are provided in samples directory
    Code will stop after 1 hour. We can restart the code using the above command again using the same list name to continue the run.
    Format for csv file: csv with columns 'name' as company name, 'details' as other details like location for the company.
    Format for desigations file : all the designations in a single column in a csv. Column name should be 'designations'.

Additional Features:
    Upload a list of linkedin company urls directly to an existing list. 
        python company_linkedin_urls_manual_insert.py -n "list name" -f "csv file loc"
        csv should have columns 'name','details' and 'linkedin_url' for company name, details about the company and linkedin url respectively.
    generate people details for email extration:
        python gen_people_for_email.py -n "list name" -d "designation csv loc"
        This will populate the "crawler.people_details_for_email_verifier" table with data crawled till that point. Then the email extraction code can use that data.