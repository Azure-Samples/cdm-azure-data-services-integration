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

Each step leverages the metadata contained in the CDM folder to make it easier and simpler to accomplish the task.  

The samples include libraries, code, and Azure resource templates that you can use with with CDM folders that you create from your own data.

IMPORTANT: the sample code is provided as-is with no warranties and is intended for learning purposes only.

## Getting Started

Download the project and open the tutorial in the documentation folder.  

### Prerequisites
See the tutorial for details.

### Installation
See the tutorial for detailed installation instructions.

