![](https://github.com/LaurentWilli/Data_Engineering/blob/main/images/nycCabProject_Header.png?raw=true)

# Project - New York City Taxi Trip records

[Apache Spark] [PySpark] [Jupyter] [Big Data] [ETL] [Machine Learning] [Parquet]

## Abstract

This project implements a fault-tolerant ETL and ML pipeline for the NYC TLC Trip Record dataset. Leveraging Apache Spark on Linux, it addresses the technical challenges of ingesting multi-year Parquet archives with evolving schemas. The solution utilizes a "divide and conquer" ingestion strategy to handle conflicting data types (Long/Double) and Parquet dictionary encoding issues without data loss.

Post-ingestion, the pipeline executes complex transformations within Jupyter Notebooks, joining trip data with taxi zone lookups for geospatial analysis. The clean dataset is subsequently used to train regression and clustering models to optimize route insights.

[![Jupyter Notebook](https://img.shields.io/badge/Jupyter-Notebook-F37626?style=for-the-badge&logo=jupyter&logoColor=white)](nyYellowCab.ipynb)