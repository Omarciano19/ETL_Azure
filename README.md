# ETL Pipeline Project on Azure Databricks with Data Lake Gen2

## Project Overview
This project demonstrates the creation of an end-to-end ETL (Extract, Transform, Load) pipeline using **Azure Databricks** and **Azure Data Lake Storage Gen2**. The pipeline extracts raw data from the data lake, performs transformations using **PySpark** and **Spark SQL**, and finally loads the cleaned and transformed data back into the data lake in **Parquet** format for future analysis.

## Table of Contents
1. [Technologies Used](#technologies-used)
2. [Architecture](#architecture)
3. [Dataset Description](#dataset-description)
4. [ETL Pipeline Steps](#etl-pipeline-steps)
5. [Setup Instructions](#setup-instructions)
6. [Results and Optimization](#results-and-optimization)
7. [Conclusion](#conclusion)

## Technologies Used
- **Azure Databricks**: For creating and running notebooks to process the data.
- **Azure Data Lake Gen2**: For storing the raw and processed data.
- **PySpark**: For transforming and processing the data in Databricks.
- **Spark SQL**: For querying the data during transformations.
- **Parquet**: As the final output data format.
- **Azure Storage Explorer**: To monitor and validate data in the Data Lake.

## Architecture
The pipeline follows a simple ETL architecture:

1. **Extract**: Raw data is loaded from a container in Azure Data Lake Gen2.
2. **Transform**: Data is cleaned and transformed using PySpark and Spark SQL.
3. **Load**: The transformed data is written back to Azure Data Lake in a different container, in **Parquet** format.

## Dataset Description
The dataset used for this project contains a datavase with information about taxi trips in Chicago:

Table `neighborhoods`: data about the city's neighborhoods:
- -`name`
- -`neighborhood_id`

Table `cabs`: data about the taxis

- -`cab_id`: code of the vehicle
- -`vehicle_id`: technical ID of the vehicle
- -`company_name`: name of the company that owns the vehicle

table  `trips`:

- -`trip_id`: trip id
- -`cab_id`: id of the vehicle
- -`start_ts`: date and time of trip start (time rounded to the hour)
- -`end_ts`: date and time of trip end (time rounded to the hour)
- -`duration_seconds`: trip duration in seconds
- -`distance_miles`: trip distance in miles
- -`pickup_location_id`: pickup neighborhood code
- -`dropoff_location_id`: dropoff neighborhood code

`weather_records` table: weather data

- -`record_id`: weather record code
- -`ts`: date and time of the record (time rounded to the hour)
- -`temperature`: temperature when the record was taken
- -`description`: brief description of weather conditions, for example, "light rain" or "scattered clouds"

## Table schema:

![image](https://github.com/user-attachments/assets/bfd64168-da6d-40bf-9d01-ab909664fe6e)

The dataset is loaded from the Azure Data Lake `container1` container and processed in the pipeline.

## ETL Pipeline Steps

### 1. Data Extraction
The raw data is loaded from Azure Data Lake using PySpark's `spark.read` function:

```python
cabs = spark.read.csv("/mnt/container1/cabs.csv", header=True, inferSchema=True)
trips = spark.read.csv("/mnt/container1/trips.csv", header=True, inferSchema=True)
neighborhoods = spark.read.csv("/mnt/container1/neighborhoods.csv", header=True, inferSchema=True)
weather_records = spark.read.csv("/mnt/container1/weather_records.csv", header=True, inferSchema=True)
datos = spark.read.csv("/mnt/container1/datos.csv", header=True, inferSchema=True)
```

### 2.Data Transformation
The transformation process includes:

- **Removing missing values**
```python 
cabs = raw_data.dropna()
trips = raw_data.dropna()
neighborhoods = raw_data.dropna()
weather_records = raw_data.dropna()
datos = raw_data.dropna()
```
- **Handling duplicate records**
```python Removing duplicates
cabs = cabs.dropDuplicates()
trips = trips.dropDuplicates()
neighborhoods = neighborhoods.dropDuplicates()
weather_records = weather_records.dropDuplicates()
datos = datos.dropDuplicates()
```

- **Spark SQL queries to filter**
```python
# Applying SparkSQL filters as:
filtro2 = spark.sql("""
        select *
        from 
            (select company_name, count(trip_id) as trips_amount 
            from cabs inner join trips on cabs.cab_id=trips.cab_id
        where company_name like '%Yellow%'  and start_ts::date between '2017-11-01' and '2017-11-07'
            group by cabs.company_name) as sub1 union
            (select company_name, count(trip_id) as trips_amount
        from cabs inner join trips on cabs.cab_id=trips.cab_id
        where company_name like '%Blue%'  and start_ts::date between '2017-11-01' and '2017-11-07'
            group by cabs.company_name) 
""")
```
- **Applying business logic transformations**
```python
res = st.levene(df_loop_ohare_good["duration_seconds"],
                df_loop_ohare_bad["duration_seconds"])
print('valor p:', res.pvalue)
alpha = 5

if (res.pvalue < alpha):
    print("Rechazamos la hipótesis nula")
else:
    print("No podemos rechazar la hipótesis nula")
```


### 3.Data Loading
The transformed data is written back to Azure Data Lake in Parquet format for efficient storage and querying:
```python
filtro1.write.mode("overwrite").parquet("/mnt/container1/sql_result_01")
filtro2.write.mode("overwrite").parquet("/mnt/container1/sql_result_02")
filtro3.write.mode("overwrite").parquet("/mnt/container1/sql_result_03")
filtro4.write.mode("overwrite").parquet("/mnt/container1/sql_result_04")
filtro5.write.mode("overwrite").parquet("/mnt/container1/sql_result_05")
filtro6.write.mode("overwrite").parquet("/mnt/container1/sql_result_06")
```

### 4.Scheduling the Pipeline
The ETL pipeline is scheduled to run weekly using Azure Databricks Jobs. The job configuration is provided in the notebooks/job_config.py file.

## Setup Instructions
### Prerequisites
- **Azure Subscription**
- **Databricks workspace created**
- **Azure Data Lake Storage Gen2 set up**

### Steps to Run the Project

1. Clone this repository:
   
```python
git clone https://https://github.com/Omarciano19/ETL_Azure
```

2. Upload the notebook files to your Databricks workspace.

3. Create a new cluster in Databricks (ensure you have enough vCPUs).
   - With a student acount I recomend using a single node configuration with a `Standard_F4` node type.

4. Configure the cluster with the necessary libraries:
    - `pyspark`
    - `azure-storage-blob`

5. Run the notebook:
    - Run the only notebook.

   In the next versión, you will need to run then sequentially:
    - `01_data_extraction.ipynb`
    - `02_data_transformation.ipynb`
    - `03_data_loading.ipynb`

6. Schedule the pipeline using **Databricks Jobs**.


## Results and Optimization

- **Data Processing Time**: The pipeline processed approximately **1 GB** of data in **3 minutes**.
- **Optimizations**:
    - Used **partitioning** and **caching** to speed up transformations.
    - Stored final output in **Parquet** format for better query performance.

---

## Conclusion

This project demonstrates how to build a scalable ETL pipeline using modern cloud technologies like Azure Databricks and Data Lake Gen2. The pipeline can be extended for real-time data processing or batch processing at larger scales.

---

## Future Improvements

- Integration with **Azure Data Factory** for more complex workflows.
- Implementing real-time data streaming using **Azure Event Hubs**.
