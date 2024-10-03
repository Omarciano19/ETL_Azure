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
The dataset used for this project contains [insert dataset description, e.g., customer transaction data, sales data, etc.]. The raw data is stored in CSV format and contains the following columns:
- `column_1`: Description of the column.
- `column_2`: Description of the column.
- `...`

The dataset is loaded from the Azure Data Lake `raw` container and processed in the pipeline.

## ETL Pipeline Steps

### 1. Data Extraction
The raw data is loaded from Azure Data Lake using PySpark's `spark.read` function:

```python
raw_data = spark.read.format("csv").option("header", "true").load("path_to_raw_data_in_data_lake")
```

### 2.Data Transformation
The transformation process includes:

- **Removing missing values**
- **Handling duplicate records**
- **Applying business logic transformations**
- **Sample transformation code**

```python
# Dropping null values
cleaned_data = raw_data.dropna()

# Removing duplicates
cleaned_data = cleaned_data.dropDuplicates()

# Applying business logic
transformed_data = cleaned_data.withColumn("new_column", some_transformation())
```
### 3.Data Loading
The transformed data is written back to Azure Data Lake in Parquet format for efficient storage and querying:
```python
transformed_data.write.format("parquet").save("path_to_processed_data_in_data_lake")
```

### 4.Scheduling the Pipeline
The ETL pipeline is scheduled to run daily using Azure Databricks Jobs. The job configuration is provided in the notebooks/job_config.py file.

## Setup Instructions
### Prerequisites
- **Azure Subscription**
- **Databricks workspace created**
- **Azure Data Lake Storage Gen2 set up**

### Steps to Run the Project

1. Clone this repository:
   
```python
git clone https://github.com/your_username/your_repo_name.git
```

2. Upload the notebook files to your Databricks workspace.

3. Create a new cluster in Databricks (ensure you have enough vCPUs).

4. Configure the cluster with the necessary libraries:
    - `pyspark`
    - `azure-storage-blob`

5. Run the notebooks sequentially:
    - `01_data_extraction.ipynb`
    - `02_data_transformation.ipynb`
    - `03_data_loading.ipynb`

6. Schedule the pipeline using **Databricks Jobs**.


## Results and Optimization

- **Data Processing Time**: The pipeline processed approximately **X GB** of data in **Y minutes**.
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
