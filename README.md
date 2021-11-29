# Vessel Project

This is a data engineering solution that reads flat files, transforms the data, uploads into an SQL database and visualizes the resulting data.

# TOOLS USED
  1. Azure storage
  2. Azure databricks
  3. Azure data factory
  4. Azure SQL server and database
  5. Power BI
  6. Parallels(virtual machine)

  ![alt text](https://github.com/adeniranwumi/vessel_project/blob/main/SOLUTION_OVERVIEW.png)


  ## STEP 1
  
  The flat files are uploaded into an Azure blob storage container
  
  
  ## STEP 2
  
  The next step is setting up a Databricks workspace to transform the data. Pyspark is the language used here. 
  This step facilitates the transformation of data into the data model and writes the data into the database.
  
   ## DATABASE STRUCTURE

  ![alt text](https://github.com/adeniranwumi/vessel_project/blob/main/Entity_relationship_diagram.png)
  
  The scripts for creating the tables and some stored procedures for data transformation can be referenced here: [SQL SCRIPTS](/scripts.sql)
  
  ## STEP 3
  
  A data factory pipeline runs the Databricks notebook along with some stored procedures for further transformation of the data.
  
   ## DATA FACTORY PIPELINE

  ![alt text](https://github.com/adeniranwumi/vessel_project/blob/main/Data_factory_pipeline.png)
  
  ## STEP 4
  
  The data is visualized using Power BI. Below are some generated reports.
  
  ![alt text](https://github.com/adeniranwumi/vessel_project/blob/main/REPORT1.png) 
  ![alt text](https://github.com/adeniranwumi/vessel_project/blob/main/REPORT2.png)
  ![alt text](https://github.com/adeniranwumi/vessel_project/blob/main/REPORT3.png)
  ![alt text](https://github.com/adeniranwumi/vessel_project/blob/main/REPORT4.png)
