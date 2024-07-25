# 1. Setting up mamba project

### 1.1 Starting from example project

- You can start by cloning project [MambaETL Starter Project](https://github.com/samuelabebayehu/openmrs-module-mambaexample)
- Inside README file there is a section explainning what changes are made to SDK generated platform module
- Next generate a JSON file using the query provided [Generate flat table JSON](https://pastebin.com/K31iBWNK)
- To explain what we are doing let’s revise mamba concept:

  [Reporting in OpenMRS - A MambaETL Showcase.pdf](https://prod-files-secure.s3.us-west-2.amazonaws.com/0b910770-d281-487f-8ea6-2ed41b2c46cf/297e9812-6d35-4def-9e38-37dd326311e7/Reporting_in_OpenMRS_-_A_MambaETL_Showcase.pdf)

- As shown above we are creating a transposed mapping of concepts (Encounter types) so the columns will be transposed automatically once we define the concept. For eg. the below json file shows followup encounter type mapped accordingly with all concepts as columns. This is auto generated using the SQL query above.

```bash
{
  "report_name": "Follow Up",
  "flat_table_name": "mamba_flat_encounter_follow_up",
  "encounter_type_uuid": "136b2ded-22a3-4831-a39a-088d35a50ef5",
  "table_columns": {
    "height": "f980e9d2-e85c-483e-ae93-980623114e6f",
    "lab_id": "051f22d7-24dc-423c-a13d-b5de1e2e8361",
    "regimen": "6d7d0327-e1f8-4246-bfe5-be1e82d94b14",
    "cd4_done": "4868dd2d-4d56-4e72-8c89-8658a32a9072",
    "adherence": "23d97715-589c-4dcf-bb86-70e26bba2269",
    "cd4_count": "54S97AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "visit_type": "b3f60308-cda4-41f9-af08-b98d2c1562c7",
    "weight_kg_": "5089AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "haemoglobin": "21AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "dsd_category": "defeb4ff-d07b-4e4a-bbd6-d4281c1384a2",
    "tpt_followup": "0166677a-5a8e-45fa-b3f6-3c5aa9f13d00",
    "biopsy_result": "df94b4c4-8a3a-46b2-be5b-e948403081a0"
    }
   
```

- Define required parameters in the project’s parent pom.xml file to setup the source schema and destination schema where the extracted data will be stored

    ```bash
    <!-- The source database (OpenMRS database) -->
    <argument>-d openmrs_v2</argument>
    <!-- The target or analysis Database where the ETL data is stored -->
    <argument>-a analysis_db_test</argument>
    ```

- Next run `mvn clean install` This will generate a file `create_stored_procedures.sql` inside `/api/resources/mamba` folder. This will create all the stored procedures required to flatten the data per our requirement. You should see the below structure inside your defined analysis schema after running it manually without deploying the module. The paired liquibase.xml will do the same when you deploy it to an openmrs server.

![Untitled](https://prod-files-secure.s3.us-west-2.amazonaws.com/0b910770-d281-487f-8ea6-2ed41b2c46cf/14e4658c-43e1-44e9-be8c-0c1338c0e581/Untitled.png)

- Next run `sp_mamba_data_processing_etl.sql` which contains the entry point stored procedure to orchestrate and run all defiled stored procedures to automatically flatten the data. This will create and populate all the tables that come from base core modules that are common structures and the ones from config folder JSON definitions. You should see tables like `mamba_z_encounter_obs` populated automatically and others like mamba_flat_encounter_followup from the config.

![Untitled](https://prod-files-secure.s3.us-west-2.amazonaws.com/0b910770-d281-487f-8ea6-2ed41b2c46cf/668df185-6842-41c9-8913-a623990ed912/Untitled.png)