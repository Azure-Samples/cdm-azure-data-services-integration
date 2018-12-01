# CDM & CDM Folders sample

Data stored in the [Common Data Model (CDM)](https://docs.microsoft.com/common-data-model) format provides semantic consistency across apps and deployments. With the evolution of the CDM metadata system, the CDM brings the same structural consistency and semantic meaning to the data stored in Azure Data Lake Storage Gen2 Preview with hierarchical namespaces and folders that contain schematized data in standard CDM format. The standardized metadata and self-describing data in an Azure data lake facilitates metadata discovery and interoperability between data producers and consumers such as Power BI, Azure Data Factory, Azure Databricks, and Azure Machine Learning service.

A "CDM folder" is a folder in the Azure Data Lake Storage Gen2, conforming to specific, well-defined and standardized metadata structures and self-describing data, to facilitates effortless metadata discovery and interop between data producers (e.g. Dynamics 365 business application suite) and data consumers, such as Power BI analytics, Azure data platform services (e.g. Azure Machine Learning, Azure Data Factory, Azure Databricks, etc.) and turn-key SaaS applications (Dynamics 365 AI for Sales, etc.) The standardized metadata is defined in the model.json file, which exists in the folder and containers pointers to the actual data file locations.

The subfolders in this directory provide a set of sample libraries and schema files to read and write the model.json file used in other samples in this account. 

The latest versions of these libraries can be found in https://github.com/Microsoft/CDM

## More information
- [CDM(https://docs.microsoft.com/common-data-model)
- [CDM folders](https://docs.microsoft.com/common-data-model/data-lake)
- [model.json](https://docs.microsoft.com/common-data-model/model-json)
