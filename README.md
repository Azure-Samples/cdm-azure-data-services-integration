# CDM folders and Azure Data Services integration

Tutorial and sample code for integrating Power BI dataflows and Azure Data Services using CDM folders in Azure Data Lake Storage Gen2.

## Features

The tutorial walks through use of CDM folders in a modern data warehouse scenario.  In it you will:
- Configure your Power BI account to save Power BI dataflows as CDM folders in ADLS Gen2;  
- Create a Power BI dataflow by ingesting order data from the Wide World Importers sample database and save it as a CDM folder;
- Use an Azure Databricks notebook that prepares and cleanses the data in the CDM folder, and then writes the updated data to a new CDM folder in ADLS Gen2;
- Use Azure Machine Learning to train and publish a model using data from the CDM folder.
- Use an Azure Data Factory pipeline to load data from the CDM folder into staging tables in Azure SQL Data Warehouse and then invoke stored procedures that transform the data into a dimensional model.
- Use Azure Data Factory to orchestrate the overall process and monitor execution.

Each step leverages the metadata contained in the CDM folder to make it easier and simpler to accomplish the task.  

The provided samples include code, libraries, and Azure resource templates that you can use with CDM folders you create from your own data.

IMPORTANT: the sample code is provided as-is with no warranties and is intended for learning purposes only.

## Getting Started

Download the project and open the tutorial in the documentation folder.  

### Prerequisites
See the tutorial for details.

### Installation
See the tutorial for detailed installation instructions.

