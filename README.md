# CDM Azure Data Services Integration

Tutorial and sample code for integrating and working with CDM folders in Azure Data Services applications.

## Features

The tutorial walks through use of CDM folders in a modern data warehouse scenario.  In it you will:
- Configure your Power BI account to save Power BI Dataflows as CDM folders in Azure Data Lake Storage Gen2;  
- Create a Power BI Dataflow by ingesting order data from the World Wide Importers sample database and save it as a CDM folder;
- Use an Azure Databricks notebook that prepares and cleanses the data in the CDM folder for later analytics processing, and then write the updated data to a new CDM folder in the data lake;
- Use Azure Machine Learning to train and publish a model using data from the CDM folder.
- Use an Azure Data Factory pipeline to Load data from the CDM folder into staging tables in Azure SQL Data Warehouse and then invoke stored procedures that transform the data into a dimensional model.
- using Azure Data Factory to orchestrate the overall process so that it runs on a schedule and monitor execution.

In each task you leverage the the metadata in the CDM folder that describes the data it contains.  

The samples include libraries, code and Azure resource templates that you can use with CDM folders that you create from your own data.

IMPORTANT: all sample code is provided as-is with no warranties and is intended for learning purposes only.

## Getting Started

Download the project and open the tutorial in the documentation folder.


The tutorial is divided into sections, one for each Azure Data Service.  Each section includes instructions for deploying and exploring the samples for that service. The Azure Data Factory section includes instructions for orchestrating Azure Databricks, Azure Machine Learning and Azure SQL Data Warehouse.    

For the full experience, you should walk through the tutorial from beginning to end, including operationalizing the flow with Aure Data Factory. You can also explore sections covering each Data Service independently.   

### Prerequisites
You must have the following to go through the tutorial: 
- A Power BI Pro or Premium account
- An Azure subscription

It is also recommended that you install:
- Azure Storage Explorer with support for Azure Data Lake Storage Gen 2, used to browse and manage CDM folders in ADLS Gen2
- Latest version of SQL Server Management Studio (SSMS) or Azure Data Studio, used to browse the SQL database and SQL data warehouse

### Installation
See the tutorial for detailed installation instructions.

