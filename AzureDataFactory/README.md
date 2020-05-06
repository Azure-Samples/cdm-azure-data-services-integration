
# OBSOLETE

For information on how to use Azure Data Factory mapping data flows to read and write CDM entity data, see [Using ADF Mapping Data Flows with CDM.pdf](https://github.com/Azure-Samples/cdm-azure-data-services-integration/blob/master/Using%20ADF%20Mapping%20Data%20Flows%20with%20CDM.pdf)

---



# Use Azure Data Factory to load data from a CDM folder into SQL Data Warehouse

This directory contains the usage details, samples and library to orchestrate your entire workflow with an Azure Data Factory pipeline.

## Content
* *adf-arm-template-cdm-to-dw* - deploy this ARM template for the data factory and all its entities for the workflow that copies data from the new CDM tolder and loads it into DW
* *adf-arm-template-databricks-cdm-to-dw/* - deploy this ARM template for the data factory and all its entities if you wish to orchestrate the _entire_ Azure Data Services flow. This pipeline will invoke the Databricks data preperation notebook as well as invoke the pipeline that copies data from the new CDM tolder and loads it into DW
tolder and loads it into DW
* *arm-template-azure-function-app* - template to deploy the function app you will need to host your azure function.
* *sample-azure-function* - you will need to deploy this Azure function that will read the entity definitions and translate it into SQL scripts to create the staging tables. This will be invoked by the data factory pipeline. See "Parse.cs" for what code the function is executing.
