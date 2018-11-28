# CDM Azure Data Services Integration

Tutorial and sample code for integrating and working with CDM folders in Azure Data Services applications.

## Features

The tutorial walks through how to integrate CDM folders in a modern data warehouse scenario, including:
- Configuring your Power BI account to save Power BI Dataflows to Azure Data Lake Storage Gen2  
- Creating a Power BI Dataflow based on the World Wide Importers database and saving the dataflow as a CDM folder in ADLS Gen2
- Creating an Azure Databricks notebook that processes the CDM folder to prepare and cleanse the data.  The updated data is written to a new CDM folder.
- Creating, training and publishing an Azure Machine Learning model using data in the CDM folder for training the model.
- Loading data from the CDM folder into staging tables in Azure SQL Data Warehouse and then transforming the data into a dimensional model. 

The samples included with the tutorial include sample libraries and other sample components that you can use with CDM folders you create with your own data.

IMPORTANT: all sample code is provided as-is with no warranties and is intended for learning purposes only.

## Getting Started

Download the project and open the tutorial guide in the Documentation folder.  Follow the getting started and instalation instructions in the guide, and then walk through the tutorial. 

The tutorial is divided into sections, one for each Azure Data Service.  Each section incudes instructions for deploying and exploring the samples for that service. The Azure Data Factory section includes instructions for orchestrating Azure Databricks, Azure Machine Learning and Azure SQL Data Warehouse.    

For the full experienceyou should walk through the tutorial from beginning to end, including operationalizang the flow with Aure Data Factory. You can also explore sections covering each Data Service independently.   

### Prerequisites
You must have the following to go through the tutorial: 
- A Power BI Pro or Premium account
- An Azure subscription
- Familiarity with the Azure portal 

It is recommended that you install the following before you start:
- Azure Storage Explorer with support for Azure Data Lake Storage Gen 2, used to browse and manage CDM folders in ADLS Gen2
- Latest version of SSMS or Azure Data Studio, used for working with Azure SQL Data Warehouse

### Installation


### Quickstart
(Add steps to get up and running quickly)

1. git clone [repository clone url]
2. cd [respository name]
3. ...

## Resources

(Any additional resources or related projects)

- Link to supporting information
- Link to similar sample
- ...
