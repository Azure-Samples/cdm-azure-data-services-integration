# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Setup: Define all the inputs variables

# COMMAND ----------

# Take as inputs the CDM folder locations. Switch to defaults if no inputs are specified

# Get values from the widgets if specified
dbutils.widgets.text("inputCDMFolderLocation", "", "InputCDMFolderLocation")
dbutils.widgets.text("outputCDMFolderLocation", "","OutputCDMFolderLocation")
inputLocation = dbutils.widgets.get("inputCDMFolderLocation")
outputLocation = dbutils.widgets.get("outputCDMFolderLocation")

# Default values if no values specified in widgets
if inputLocation == '':
   inputLocation = "https://cdsabyosadev01dxt.dfs.core.windows.net/powerbi/WideWorldImporters-1/WideWorldImporters-Orders/model.json"

if outputLocation == '':
   outputLocation = "https://cdsabyosadev01dxt.dfs.core.windows.net/powerbi/PremalTest/wideworldimportersdemo"

# Parameters to authenticate to ADLS Gen 2
appID = "bd8e91da-c58c-4407-95e0-a8ecad012fc7"
appKey = "Lky5pTg2teniHdbeTlmOuivjdIPBPcwMQJ49wGwKiXA="
tenantID = "72f988bf-86f1-41af-91ab-2d7cd011db47"

# COMMAND ----------

salesOrderDf = (spark.read.format("com.microsoft.cdm")
                     .option("cdmModel", inputLocation)
                     .option("entity", "Sales Orders")
                     .option("appId", appID)
                     .option("appKey", appKey)
                     .option("tenantId", tenantID)
                     .load())

# COMMAND ----------

salesOrderLinesDf = (spark.read.format("com.microsoft.cdm")
                     .option("cdmModel", inputLocation)
                     .option("entity", "Sales OrderLines")
                     .option("appId", appID)
                     .option("appKey", appKey)
                     .option("tenantId", tenantID)
                     .load())

# COMMAND ----------

salesCustomerDf = (spark.read.format("com.microsoft.cdm")
                     .option("cdmModel", inputLocation)
                     .option("entity", "Sales Customers")
                     .option("appId", appID)
                     .option("appKey", appKey)
                     .option("tenantId", tenantID)
                     .load())

# COMMAND ----------

salesCustomerCategoriesDf = (spark.read.format("com.microsoft.cdm")
                     .option("cdmModel", inputLocation)
                     .option("entity", "Sales CustomerCategories")
                     .option("appId", appID)
                     .option("appKey", appKey)
                     .option("tenantId", tenantID)
                     .load())

# COMMAND ----------

salesBuyingGroupsDf = (spark.read.format("com.microsoft.cdm")
                     .option("cdmModel", inputLocation)
                     .option("entity", "Sales BuyingGroups")
                     .option("appId", appID)
                     .option("appKey", appKey)
                     .option("tenantId", tenantID)
                     .load())

# COMMAND ----------

warehouseStockItemsDf = (spark.read.format("com.microsoft.cdm")
                              .option("cdmModel", inputLocation)
                              .option("entity", "Warehouse StockItems")
                              .option("appId", appID)
                              .option("appKey", appKey)
                              .option("tenantId", tenantID)
                              .load())

# COMMAND ----------

warehouseColorsDf = (spark.read.format("com.microsoft.cdm")
                          .option("cdmModel", inputLocation)
                          .option("entity", "Warehouse Colors")
                          .option("appId", appID)
                          .option("appKey", appKey)
                          .option("tenantId", tenantID)
                          .load())

# COMMAND ----------

warehousePackageTypesDf = (spark.read.format("com.microsoft.cdm")
                                .option("cdmModel", inputLocation)
                                .option("entity", "Warehouse PackageTypes")
                                .option("appId", appID)
                                .option("appKey", appKey)
                                .option("tenantId", tenantID)
                                .load())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Populate the buying group entity with unassigned

# COMMAND ----------

salesBuyingGroupsDf.printSchema()

# COMMAND ----------

display(salesBuyingGroupsDf)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType
from pyspark.sql.functions import to_date, lit

unassignedBuyingGroupDf = spark.sql("select -1, 'Unassigned', 0, to_date('2013-01-01 00:00:00.0000000', 'yyyy-MM-dd H:mm:ss.SSSSSSS'), to_date('9999-12-31 23:59:59', 'yyyy-MM-dd H:mm:ss')")

newSalesBuyingGroupsDf = salesBuyingGroupsDf.union(unassignedBuyingGroupDf)

display(newSalesBuyingGroupsDf)

# COMMAND ----------

newSalesBuyingGroupsDf.printSchema()

# COMMAND ----------

display(newSalesBuyingGroupsDf)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Process customer and replace NULL buying group with -1 (Unassigned)

# COMMAND ----------

from pyspark.sql.functions import col, lit

salesCustomerDf.filter(col("BuyingGroupId").isNull()).count()

# COMMAND ----------

newSalesCustomerDf = salesCustomerDf.fillna({'BuyingGroupId' : -1})

# COMMAND ----------

newSalesCustomerDf.filter(col("BuyingGroupId").isNull()).count()

# COMMAND ----------

display(newSalesCustomerDf)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Add a hash column for history tracking

# COMMAND ----------

from pyspark.sql.functions import concat

hashedSalesCustomerDf = (newSalesCustomerDf.withColumn("ChangeTrackingHash", 
                                                       concat(col("BuyingGroupID"),
                                                              col("StandardDiscountPercentage"),
                                                              col("IsOnCreditHold"),
                                                              col("DeliveryPostalCode"))))

display(hashedSalesCustomerDf)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Exclude corporate customers

# COMMAND ----------

hashedSalesCustomerDf.createOrReplaceTempView("hashedSalesCustomer")
salesOrderDf.createOrReplaceTempView("salesOrders")
salesBuyingGroupsDf.createOrReplaceTempView("salesBuyingGroups")
salesCustomerCategoriesDf.createOrReplaceTempView("salesCustomerCategories")


# COMMAND ----------

corporateSalesCustomerDf = spark.sql("select * from hashedSalesCustomer c, salesCustomerCategories cc where c.customerCategoryID = cc.customerCategoryID and cc.CustomerCategoryName != 'Corporate'")

# COMMAND ----------

display(corporateSalesCustomerDf)
print(corporateSalesCustomerDf.count())

# COMMAND ----------

corporateSalesCustomerDf.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Write out all the entities

# COMMAND ----------

# Specify the CDM model name to output
cdmModelName = "Transformed-Wide-World-Importers"

# COMMAND ----------

(salesOrderDf.write.format("com.microsoft.cdm")
             .option("entity", "Sales Orders")
             .option("appId", appID)
             .option("appKey", appKey)
             .option("tenantId", tenantID)
             .option("cdmFolder", outputLocation)
             .option("cdmModelName", cdmModelName)
             .save())

# COMMAND ----------

(salesOrderLinesDf.write.format("com.microsoft.cdm")
             .option("entity", "Sales OrderLines")
             .option("appId", appID)
             .option("appKey", appKey)
             .option("tenantId", tenantID)
             .option("cdmFolder", outputLocation)
             .option("cdmModelName", cdmModelName)
             .save())

# COMMAND ----------

(corporateSalesCustomerDf.write.format("com.microsoft.cdm")
             .option("entity", "Corporate Sales Customers")
             .option("appId", appID)
             .option("appKey", appKey)
             .option("tenantId", tenantID)
             .option("cdmFolder", outputLocation)
             .option("cdmModelName", cdmModelName)
             .save())

corporateSalesCustomerDf.count()

# COMMAND ----------

(salesCustomerCategoriesDf.write.format("com.microsoft.cdm")
             .option("entity", "Sales CustomerCategories")
             .option("appId", appID)
             .option("appKey", appKey)
             .option("tenantId", tenantID)
             .option("cdmFolder", outputLocation)
             .option("cdmModelName", cdmModelName)
             .save())

# COMMAND ----------

(newSalesBuyingGroupsDf.write.format("com.microsoft.cdm")
             .option("entity", "Sales BuyingGroups")
             .option("appId", appID)
             .option("appKey", appKey)
             .option("tenantId", tenantID)
             .option("cdmFolder", outputLocation)
             .option("cdmModelName", cdmModelName)
             .save())

# COMMAND ----------

(warehouseStockItemsDf.write.format("com.microsoft.cdm")
             .option("entity", "Warehouse StockItems")
             .option("appId", appID)
             .option("appKey", appKey)
             .option("tenantId", tenantID)
             .option("cdmFolder", outputLocation)
             .option("cdmModelName", cdmModelName)
             .save())

# COMMAND ----------

(warehouseColorsDf.write.format("com.microsoft.cdm")
             .option("entity", "Warehouse Colors")
             .option("appId", appID)
             .option("appKey", appKey)
             .option("tenantId", tenantID)
             .option("cdmFolder", outputLocation)
             .option("cdmModelName", cdmModelName)
             .save())

# COMMAND ----------

(warehousePackageTypesDf.write.format("com.microsoft.cdm")
             .option("entity", "Warehouse PackageTypes")
             .option("appId", appID)
             .option("appKey", appKey)
             .option("tenantId", tenantID)
             .option("cdmFolder", outputLocation)
             .option("cdmModelName", cdmModelName)
             .save())
