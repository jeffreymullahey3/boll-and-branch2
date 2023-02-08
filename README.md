This is the dbt project I created for the Boll & Branch interviewing process.

I was not able to connect to the data-recruiting project, so I copied the source tables into a new project 'bb-dc-1'.

In order to run these models against the source tables in the data-recruiting project, you would need
to edit the dbt database, schema, and table names in the src_ae_data_challenge.yml file in the models/staging folder.

All ad hoc queries are contained in the analyses/adhoc_questions folder.

All data quality checks are located in the analyses/data_checks folder.