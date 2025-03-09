# databricks-end-to-end-project


Databricks Platform offers a variety of languages to perform data engineering tasks. The goal of this project is to Design and implement a Lakehouse in Databricks using a medallion architecture pattern. We will collect and ingest data from source system, transform, test and make it available for data consumers.


## What is this project about?

This is an end to end project, that will help us go over the different aspect of <b> databricks</b> integration, following a medallion architecture.

![image](https://github.com/tmbothe/Data-Engineering-with-databricks/blob/main/images/dbt_project.jpg)

As we can see on the image above, the whole pipeline is typical Data engineering project that:
* Data coming from external sources is loaded into a **landing zone** in  Azure blob storage
* Then we will read data as is from the landing zone to the **bronze zone** with with a container in Azure
* After performing transformations and cleaning we will store the clean data into the **Silver zone**
* We will finally perform some aggrations and store the final data in the **gold zone** ready for reporting.
* All data and access to the databricks platform will be managed by the Unity catalog.
* All steps will include unit tests
* We will add an integration test at the end to make sure we can test our application end to end in a dynamic environment.

## Steps to follow
*  Have or create an Azure account. Follow steps [click here](https://learn.microsoft.com/en-us/azure/databricks/scenarios/quickstart-create-databricks-workspace-vnet-injection)
*  Create a databricks workspace and be admin to perform all tasks

## Datasets

The dataset we are going to be using is a collections of roads and traffic data that can be find on kaggle. It is basically the count of the number of vehicles that were collected at a given location at a specific time. : these vehicles are:
* Pedal cycle
* Two wheeler motor vehicles
* Buses amd coaches
* LVG (Large goods vehicle)
* HGV (Heavy Goods Vehicle)
* Electric vehicles

Here is the over all structure of the data.

![image](https://github.com/tmbothe/Data-Engineering-with-databricks/blob/main/images/schema.jpg)

## Project Implementation 

 As mentioned ealier, we are going to be using the medallion architecture for this project. We are going to build model for each step in the process.

 ### storage design 
 ![image](https://github.com/tmbothe/Data-Engineering-with-databricks/blob/main/images/storage_desing.jpg)
  
For storage, we will be using Azure ADLS Gen2 storage. We will be creating 3 diffent containers in Azure blob storage:
* **Unity catalog root** is automatically created when a databricks workspace is created in Azure platfrom and store all project metadata
* **Medallion**  this is the container that will be storing all managed ojects in different layers. We will create 3 diffent subfolders (bronze, silver, gold)
* **Umnmanage storage** will have two containers, one for the landing data coming in, and a checkpoint location to manage incremental load.

![image](https://github.com/tmbothe/Data-Engineering-with-databricks/blob/main/images/initial_project.jpg)

Our project catalog in databricks is called **dbt_project_catalog**. As we can see on the image above, it currently has only the data from our landing zone. We are going to start adding the other layers through dbt models.

### dbt models
In dbt, models are only transformation applied to the data. It is a combination of select statement added to a file that get materialized to the target system as a table or view.

#### project configuration 
Before creating the different layers, one key file for any dbt project id the **dbt_project.yml** which define how all the objects in our different layers are going to be materialized. Below is a snapshot of our project configuration. As we can see, all our ojects will be materialized as tables.
```
models:
  dbt_project_catalog:
    bronze:
      +materialized: table
      +schema: bronze
    silver:
      +materialized: table
      +schema: silver
    gold:
      +materialized: table
      +schema: gold
``` 
#### Data sources
The data sources defines where the data is originated.  We will define two data sources that represent the two tables in our landing zone.

#### Data layers
The code for different layer can be found in the project under **models**, but below is the final structure in dbt.


![image](https://github.com/tmbothe/Data-Engineering-with-databricks/blob/main/images/project_structure_in_dbt.jpg)

#### Ensuring data quality with tests

The best way is to make data quality a real thing and not just hope it defines exactly what we expect as input and output and then check that the data adheres to these requirements. This is the essence of being able to define runnable tests on our data. dbt allows two types of tests. Generic tests and singular tests.

**Generic tests**, also called schema tests, are tests that are generic in nature and are therefore codified in a parametrized query, which can be applied to any model or any column in any model by naming them in a YAML configuration file.
```
version: 2

models:
  - name: bonze_roads
    columns:
      - name: Road_category
        tests:
          - accepted_values:
              values: ['TM','PA','TA','M','PM']

      - name:  Road_ID
        tests:
          - unique
          - not_null
  - name: bronze_traffic
    columns:
      - name: Record_ID
        tests:
          - unique
          - not_null
```
Here is an example of generic test, that checks if columns are unique or not null. The one in the column **Road category** contraints the values of that columns.

**Singular tests** are the most generic form of test, as you can test everything that you can express in SQL. You just need to write a SQL query that returns the rows that do not pass your test, no matter what you would like to test, whether individual calculations, full table matches, or subsets of columns in multiple tables.

```
SELECT *
FROM {{ ref("bronze_roads") }}
WHERE Total_Link_Length_Km<0 
   or Total_Link_Length_Miles<0
   or All_Motor_Vehicles <0
```

in this example we just test that the values of the columns cannot take a negative values.

#### Implementing SCD(slowly changing dimension)
We can implement SCD type 2 using the concept on snapshots. This can be useful if we want to tract changes overtime, or reduce the transformation time as we will be processing only new incoming rows.
We want to track the different data coming into the traffic table as it is the one that get updated frequently. The full code can be found in the project under **snapshots** folder.

After bulding all layers, below the the over all lineage in dbt.

![image](https://github.com/tmbothe/Data-Engineering-with-databricks/blob/main/images/full_project_dag.jpg)

Below is the final project snapshot in databricks.

![image](https://github.com/tmbothe/Data-Engineering-with-databricks/blob/main/images/project_snapshot_databricks.jpg)

#### Project deployment
