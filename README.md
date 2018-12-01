# CDM Azure Data Services Integration

Tutorial and sample code for integrating and working with CDM folders in Azure Data Services applications.

## Features

The tutorial walks through use of CDM folders in a modern data warehouse scenario, including:
- Configuring your Power BI account to save Power BI Dataflows as CDM folders in Azure Data Lake Storage Gen2  
- Creating a Power BI Dataflow based on the World Wide Importers sample database and saving the dataflow as a CDM folder
- Creating an Azure Databricks notebook that processes the CDM folder to prepare and cleanse the data. The updated data is written to a new CDM folder.
- Creating, training and publishing an Azure Machine Learning model using data in the CDM folder for training the model.
- Loading data from the CDM folder into staging tables in Azure SQL Data Warehouse and transforming the data into a dimensional model. 

The samples include libraries, code and other components that you can use with CDM folders you create from your own data.

IMPORTANT: all sample code is provided as-is with no warranties and is intended for learning purposes only.

## Getting Started

Download the project and open the tutorial in the documentation folder. 

The tutorial is divided into sections, one for each Azure Data Service.  Each section incudes instructions for deploying and exploring the samples for that service. The Azure Data Factory section includes instructions for orchestrating Azure Databricks, Azure Machine Learning and Azure SQL Data Warehouse.    

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

