# CDM Azure Data Services Integration

Tutorial and sample code for integrating and working with CDM folders in Azure Data Services applications.

## Features

The tutorial walks through how to integrate CDM folders in a modern data warehouse scenario, including:
- Configuring your Power BI account to save Power BI Dataflows to Azure Data Lake Storage Gen2  
- Creating a Power BI Dataflow based on the World Wide Importers database and saving the dataflow as a CDM folder in ADLS Gen2
- Creating an Azure Databricks notebook that processes the CDM folder to prepare and cleanse the data.  The updated data is written to a new CDM folder.
- Creating, training and publishing an Azure Machine Learning model using data in the CDM folder for training the model.
- Loading data from the CDM folder into staging tables in Azure SQL Data Warehouse and then transforming the data into a dimensional model. 

## Getting Started

Download the project and open the tutorial guide in the Documentation folder.  Follow the getting started and instalation instructions in the guide, and then walkthrough the tutorial. 

The tutorial is divided in sections, one for each Azure Data Service.  Each section incudes instructions for installing and exporing the samples for that service. The Azure Data Factory section includes instructions for orchestrating Azure Databricks, Azure Machine Learning and Azure SQL Data Warehouse.    

You can explore each section and Data Service in isolation, or for the full expereince, walk through the tutorial from beginning to end, including operationalizang the flow with Aure Data Factory.  

### Prerequisites

- A Power BI Pro or Premium account
- An Azure subscription

It is recommended that you install the following before you start
- Latest version of SSMS or Azure Data Studio for working with Azure SQL Data Warehouse
- Azure Storage Explorer with support for Azure Data Lake Storage Gen 2

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
