---
page_type: sample
languages:
  - csharp
  - python
  - ruby
  - powershell
products:
  - azure
description: "Tutorial and sample code for integrating Power BI dataflows and Azure Data Services using Common Data Model folders in Azure Data Lake."
---

# OBSOLETE

The technology used in this tutorial is now obsolete.  

For information on using Azure Data Factory mapping data flows to read and write CDM entity data, see [Using ADF Mapping Data Flows with CDM.pdf](https://github.com/Azure-Samples/cdm-azure-data-services-integration/blob/master/Using%20ADF%20Mapping%20Data%20Flows%20with%20CDM.pdf)

For information on the new Spark CDM Connector for use in Azure Databricks and Synapse to read and write CDM entity data, see [https://github.com/Azure/spark-cdm-connector](https://github.com/Azure/spark-cdm-connector)

# OBSOLETE

# CDM folders and Azure Data Services integration

Tutorial and sample code for integrating Power BI dataflows and Azure Data Services using Common Data Model (CDM) folders in Azure Data Lake Storage Gen2.  For more information on the scenario, see this [blog post](https://azure.microsoft.com/en-us/blog/power-bi-and-azure-data-services-dismantle-data-silos-and-unlock-insights).

## Features

The [tutorial](https://github.com/Azure-Samples/cdm-azure-data-services-integration/blob/master/Tutorial/CDM-Azure-Data-Services-Integration-Tutorial.md) walks through use of CDM folders in a modern data warehouse scenario.  In it you will:
- Configure your Power BI account to save Power BI dataflows as CDM folders in ADLS Gen2;  
- Create a Power BI dataflow by ingesting order data from the Wide World Importers sample database and save it as a CDM folder;
- Use an Azure Databricks notebook that prepares and cleanses the data in the CDM folder, and then writes the updated data to a new CDM folder in ADLS Gen2;
- Use Azure Machine Learning to train and publish a model using data from the CDM folder;
- Use an Azure Data Factory pipeline to load data from the CDM folder into staging tables in Azure SQL Data Warehouse and then invoke stored procedures that transform the data into a dimensional model;
- Use Azure Data Factory to orchestrate the overall process and monitor execution.

Each step leverages metadata contained in the CDM folder to make it easier and simpler to accomplish the task.  

The provided samples include code, libraries, and Azure resource templates that you can use with CDM folders you create from your own data.

IMPORTANT: the sample code is provided as-is with no warranties and is intended for learning purposes only.

## Getting Started

See the [tutorial](https://github.com/Azure-Samples/cdm-azure-data-services-integration/blob/master/Tutorial/CDM-Azure-Data-Services-Integration-Tutorial.md) for pre-requisites and installation details.

## License
The sample code and tutorials in this project are licensed under the MIT license. See the [LICENSE](https://github.com/Azure-Samples/cdm-azure-data-services-integration/blob/master/LICENSE.md) file for more details.

## Contributing

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
