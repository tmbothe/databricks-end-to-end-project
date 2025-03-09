# databricks-end-to-end-project


Databricks Platform offers a variety of languages to perform data engineering tasks. The goal of this project is to Design and implement a Lakehouse in Databricks using a medallion architecture pattern. We will collect and ingest data from source system, transform, test and make it available for data consumers.


## What is this project about?

This is an end to end project, that will help us go over the different aspect of <b> databricks</b> integration, following a medallion architecture.

![image](https://github.com/tmbothe/databricks-end-to-end-project/blob/main/images/dbt_project.jpg)

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

![image](https://github.com/tmbothe/databricks-end-to-end-project/blob/main/images/schema.jpg)

## Project Implementation 

 As mentioned ealier, we are going to be using the medallion architecture for this project. We are going to build model for each step in the process.

 ### storage design 
 ![image](https://github.com/tmbothe/databricks-end-to-end-project/blob/main/images/storage_desing.jpg)
  
For storage, we will be using Azure ADLS Gen2 storage. We will be creating 3 diffent containers in Azure blob storage:
* **Unity catalog root** is automatically created when a databricks workspace is created in Azure platfrom and store all project metadata
* **Medallion**  this is the container that will be storing all managed ojects in different layers. We will create 3 diffent subfolders (bronze, silver, gold)
* **Umnmanage storage** will have two containers, one for the landing data coming in, and a checkpoint location to manage incremental load.

This structure is for the development environment, but we can follow the same pattern for test and produnction

### Setting  storage and external locations
We will have created all containers in Azure, and created all external storage for our different layers. The code can be found in **01-initial-config** in the repos.
We have class that will be getting all external location url and make available to all layers.
Since the intention is to be able to deploy in diffent environment, we have also designed a widdget to capture the environment that can be Dev, test or production.

```
class InitialConfig():
    def __init__(self):
        self.landing_path= spark.sql("DESCRIBE EXTERNAL LOCATION `landing`").select("url").collect()[0][0] 
        self.checkpoint_path= spark.sql("DESCRIBE EXTERNAL LOCATION `checkpoint`").select("url").collect()[0][0]
        self.silver_path= spark.sql("DESCRIBE EXTERNAL LOCATION `silver`").select("url").collect()[0][0]
        self.bronze_path= spark.sql("DESCRIBE EXTERNAL LOCATION `bronze`").select("url").collect()[0][0]
        self.gold_path= spark.sql("DESCRIBE EXTERNAL LOCATION `gold`").select("url").collect()[0][0]
        self.root_path = spark.sql("DESCRIBE EXTERNAL LOCATION `sbit-managed`").select("url").collect()[0][0]
``` 
this code will display all the location as below:
 ![image](https://github.com/tmbothe/databricks-end-to-end-project/blob/main/images/init_location.jpg)

 ### Initial setup
 In the initial setup notebook **02-initial-setup.ipynb**, we have designed different functions to perform the tasks below:
 * class to set environment and locations
 * create catalog (dev,qa, prod)
 * create schemas (bronze, silver, gold)
 * create initals tables in the bronze layer (raw_taffic and raw_roads)
 * a function to test that the environment is setup properly
 * a function to validate that all tables were created
 
 below is the code to call all functions above. The implementation can be found in the notebook
 ```
   def initial_setup(self):
        import time 
        print(f"Initializing the environment. Please wait...")
        self.create_catalog()
        self.create_bronze_Schema()
        self.create_silver_Schema()
        self.create_gold_Schema()
        self.create_roads_table()
        self.create_raw_traffic_table()
        print(f"Environment initialized.")
 ```

### Bronze layer ingestion 
The bronze layer will read data directly from the landing zone using **structure streaming** then load data in the bronze tables.
*  We first read steam for traffic and roads data
*  write the stream in bronze tables
*  validate count

```
   def load_bronze_tables(self):
        print('Loading bronze tables...', end='')
        df_traffic = self.read_raw_traffic()
        df_roads = self.read_raw_roads()
        self.write_raw_traffic(df_traffic)
        self.write_raw_roads(df_roads)
        print('Loading bronze tables completed')
``` 

### Silver layer ingestion
The silver layer is implemented using the same pattern that we used in the bronze layer, but few transformations are added and data store in silver schema.
*  read from bronze layer
*  transform road categrory
*  transform road type
*  remove duplicates
*  fill null with appropriate string and numeric values
*  compute count for electric vehicles
*  computue count for motor vehicles
*  write final datafrome to the silver shema

```
    def load_silver_traffic(self, table_name='silver.traffic_data'):
        print(f'Loading silver table {table_name}')
        df=self.read_bronze_traffic()
        df=self.remove_duplicates(df)
        columns= df.schema.names
        df=self.remove_nulls(df,columns)
        df_clean=self.ev_count_add(df)
        df_clean=self.motor_count_add(df_clean)
        df_clean=self.create_transformed_time(df_clean)
        self.write_silver_traffic(df_clean)
        print(f'Loading silver table {table_name} completed')

    def load_silver_roads(self, table_name='silver.roads_data'):
        print(f'Loading silver table {table_name}')
        df=self.read_bronze_roads()
        df=self.remove_duplicates(df)
        columns= df.schema.names
        df=self.remove_nulls(df,columns)
        df_clean=self.create_transformed_time(df)
        df=self.road_Category(df)
        df=self.road_Type(df)
        self.write_silver_roads(df)
        print(f'Loading silver table {table_name} completed')

    def load_silver_tables(self):
        print('Loading silver tables')
        self.load_silver_traffic()
        self.load_silver_roads()
        print('Loading silver tables completed')
```

### Gold layer ingestion 
the code can be found in **05-gold-loader.ipynb**
```
  def load_gold_traffic(self):
      print("Loading gold traffic")
      df = self.read_silver_traffic()
      df = self.create_vehicle_intensity(df)
      df = self.create_load_time(df)
      self.write_gold_traffic(df)
      print("Gold traffic loaded")


    def load_gold_roads(self):
      print("Loading gold roads")
      df = self.read_silver_roads()
      df = self.create_load_time(df)
      self.write_gold_roads(df)
      print("Gold roads loaded")

    def load_gold_tables(self):
      print("Loading gold tables")
      self.load_gold_traffic()
      self.load_gold_roads()
      print("Gold tables loaded")
```

### integration testing 
After bulding all different layers and test individually, we can setup another notebook that can dynamically create a catalog, create all the tables, ingest the data and test that the result matches our expectations
the code can be found in **07-streaming-integration-test.ipynb**.

![image](https://github.com/tmbothe/databricks-end-to-end-project/blob/main/images/end-end-testing.jpg)

after running the code above we see that: 

![image](https://github.com/tmbothe/databricks-end-to-end-project/blob/main/images/end_end_qa.jpg)

We can see that our **qa** environment was created, all tables in the bronze, silver and gold created. We can also see an snapshot of the data in the gold environment.

### Job workflow scheduling 

![image](https://github.com/tmbothe/databricks-end-to-end-project/blob/main/images/workflow.jpg)

We can also set up a workflow for our notebooks to make sure all layers are well populated.