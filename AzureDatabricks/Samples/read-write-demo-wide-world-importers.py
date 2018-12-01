# Databricks notebook source
salesOrderDf = (spark.read.format("com.microsoft.cdm")
                    .option("cdmModel", "https://cdsabyosadev01dxt.dfs.core.windows.net/powerbi/WideWorldImporters-1/WideWorldImporters-Orders/model.json")
                    .option("entity", "Sales Orders")
                    .option("appId", "bd8e91da-c58c-4407-95e0-a8ecad012fc7")
                    .option("appKey", "Lky5pTg2teniHdbeTlmOuivjdIPBPcwMQJ49wGwKiXA=")
                    .option("tenantId", "72f988bf-86f1-41af-91ab-2d7cd011db47")
                    .load())

# COMMAND ----------

salesOrderLinesDf = (spark.read.format("com.microsoft.cdm")
                     .option("cdmModel", "https://cdsabyosadev01dxt.dfs.core.windows.net/powerbi/WideWorldImporters-1/WideWorldImporters-Orders/model.json")
                     .option("entity", "Sales OrderLines")
                     .option("appId", "bd8e91da-c58c-4407-95e0-a8ecad012fc7")
                     .option("appKey", "Lky5pTg2teniHdbeTlmOuivjdIPBPcwMQJ49wGwKiXA=")
                     .option("tenantId", "72f988bf-86f1-41af-91ab-2d7cd011db47")
                     .load())

# COMMAND ----------

salesCustomerDf = (spark.read.format("com.microsoft.cdm")
                     .option("cdmModel", "https://cdsabyosadev01dxt.dfs.core.windows.net/powerbi/WideWorldImporters-1/WideWorldImporters-Orders/model.json")
                     .option("entity", "Sales Customers")
                     .option("appId", "bd8e91da-c58c-4407-95e0-a8ecad012fc7")
                     .option("appKey", "Lky5pTg2teniHdbeTlmOuivjdIPBPcwMQJ49wGwKiXA=")
                     .option("tenantId", "72f988bf-86f1-41af-91ab-2d7cd011db47")
                     .load())

# COMMAND ----------

salesCustomerCategoriesDf = (spark.read.format("com.microsoft.cdm")
                     .option("cdmModel", "https://cdsabyosadev01dxt.dfs.core.windows.net/powerbi/WideWorldImporters-1/WideWorldImporters-Orders/model.json")
                     .option("entity", "Sales CustomerCategories")
                     .option("appId", "bd8e91da-c58c-4407-95e0-a8ecad012fc7")
                     .option("appKey", "Lky5pTg2teniHdbeTlmOuivjdIPBPcwMQJ49wGwKiXA=")
                     .option("tenantId", "72f988bf-86f1-41af-91ab-2d7cd011db47")
                     .load())

# COMMAND ----------

salesBuyingGroupsDf = (spark.read.format("com.microsoft.cdm")
                     .option("cdmModel", "https://cdsabyosadev01dxt.dfs.core.windows.net/powerbi/WideWorldImporters-1/WideWorldImporters-Orders/model.json")
                     .option("entity", "Sales BuyingGroups")
                     .option("appId", "bd8e91da-c58c-4407-95e0-a8ecad012fc7")
                     .option("appKey", "Lky5pTg2teniHdbeTlmOuivjdIPBPcwMQJ49wGwKiXA=")
                     .option("tenantId", "72f988bf-86f1-41af-91ab-2d7cd011db47")
                     .load())

# COMMAND ----------

warehouseStockItemsDf = (spark.read.format("com.microsoft.cdm")
                              .option("cdmModel", "https://cdsabyosadev01dxt.dfs.core.windows.net/powerbi/WideWorldImporters-1/WideWorldImporters-Orders/model.json")
                              .option("entity", "Warehouse StockItems")
                              .option("appId", "bd8e91da-c58c-4407-95e0-a8ecad012fc7")
                              .option("appKey", "Lky5pTg2teniHdbeTlmOuivjdIPBPcwMQJ49wGwKiXA=")
                              .option("tenantId", "72f988bf-86f1-41af-91ab-2d7cd011db47")
                              .load())

# COMMAND ----------

warehouseColorsDf = (spark.read.format("com.microsoft.cdm")
                          .option("cdmModel", "https://cdsabyosadev01dxt.dfs.core.windows.net/powerbi/WideWorldImporters-1/WideWorldImporters-Orders/model.json")
                          .option("entity", "Warehouse Colors")
                          .option("appId", "bd8e91da-c58c-4407-95e0-a8ecad012fc7")
                          .option("appKey", "Lky5pTg2teniHdbeTlmOuivjdIPBPcwMQJ49wGwKiXA=")
                          .option("tenantId", "72f988bf-86f1-41af-91ab-2d7cd011db47")
                          .load())

# COMMAND ----------

warehousePackageTypesDf = (spark.read.format("com.microsoft.cdm")
                                .option("cdmModel", "https://cdsabyosadev01dxt.dfs.core.windows.net/powerbi/WideWorldImporters-1/WideWorldImporters-Orders/model.json")
                                .option("entity", "Warehouse PackageTypes")
                                .option("appId", "bd8e91da-c58c-4407-95e0-a8ecad012fc7")
                                .option("appKey", "Lky5pTg2teniHdbeTlmOuivjdIPBPcwMQJ49wGwKiXA=")
                                .option("tenantId", "72f988bf-86f1-41af-91ab-2d7cd011db47")
                                .load())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Populate the buying group entity with unassigned

# COMMAND ----------

display(salesBuyingGroupsDf)

# COMMAND ----------

unassignedBuyingGroupDf = spark.createDataFrame([(-1, 'Unassigned', 0, '2013-01-01', '9999-12-31')])
newSalesBuyingGroupsDf = salesBuyingGroupsDf.union(unassignedBuyingGroupDf)

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

def hash_function(val1, val2):
  return hash(val1 + val2)

from pyspark.sql.functions import udf

hash_function_udf = udf(hash_function)

# COMMAND ----------

newSalesCustomerDf.printSchema()

# COMMAND ----------

from pyspark.sql.functions import md5

hashedSalesCustomerDf = newSalesCustomerDf.withColumn("ChangeTrackingHash", hash_function_udf(newSalesCustomerDf.BuyingGroupID, newSalesCustomerDf.CustomerID))

# COMMAND ----------

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

corporateSalesCustomerDf = spark.sql("select * from hashedSalesCustomer c, salesCustomerCategories cc where c.customerCategoryID = cc.customerCategoryID and cc.CustomerCategoryName = 'Corporate'")

# COMMAND ----------

display(corporateSalesCustomerDf)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Write out all the entities

# COMMAND ----------

(salesOrderDf.write.format("com.microsoft.cdm")
             .option("entity", "Sales Orders")
             .option("appId", "bd8e91da-c58c-4407-95e0-a8ecad012fc7")
             .option("appKey", "Lky5pTg2teniHdbeTlmOuivjdIPBPcwMQJ49wGwKiXA=")
             .option("tenantId", "72f988bf-86f1-41af-91ab-2d7cd011db47")
             .option("cdmFolder", "https://cdsabyosadev01dxt.dfs.core.windows.net/powerbi/MatthewTest/wideworldimportersdemo")
             .option("cdmModelName", "Transformed-Wide-World-Importers")
             .save())

# COMMAND ----------

(salesOrderLinesDf.write.format("com.microsoft.cdm")
             .option("entity", "Sales OrderLines")
             .option("appId", "bd8e91da-c58c-4407-95e0-a8ecad012fc7")
             .option("appKey", "Lky5pTg2teniHdbeTlmOuivjdIPBPcwMQJ49wGwKiXA=")
             .option("tenantId", "72f988bf-86f1-41af-91ab-2d7cd011db47")
             .option("cdmFolder", "https://cdsabyosadev01dxt.dfs.core.windows.net/powerbi/MatthewTest/wideworldimportersdemo")
             .option("cdmModelName", "Transformed-Wide-World-Importers")
             .save())

# COMMAND ----------

(corporateSalesCustomerDf.write.format("com.microsoft.cdm")
             .option("entity", "Corporate Sales Customers")
             .option("appId", "bd8e91da-c58c-4407-95e0-a8ecad012fc7")
             .option("appKey", "Lky5pTg2teniHdbeTlmOuivjdIPBPcwMQJ49wGwKiXA=")
             .option("tenantId", "72f988bf-86f1-41af-91ab-2d7cd011db47")
             .option("cdmFolder", "https://cdsabyosadev01dxt.dfs.core.windows.net/powerbi/MatthewTest/wideworldimportersdemo")
             .option("cdmModelName", "Transformed-Wide-World-Importers")
             .save())

# COMMAND ----------

(salesCustomerCategoriesDf.write.format("com.microsoft.cdm")
             .option("entity", "Sales CustomerCategories")
             .option("appId", "bd8e91da-c58c-4407-95e0-a8ecad012fc7")
             .option("appKey", "Lky5pTg2teniHdbeTlmOuivjdIPBPcwMQJ49wGwKiXA=")
             .option("tenantId", "72f988bf-86f1-41af-91ab-2d7cd011db47")
             .option("cdmFolder", "https://cdsabyosadev01dxt.dfs.core.windows.net/powerbi/MatthewTest/wideworldimportersdemo")
             .option("cdmModelName", "Transformed-Wide-World-Importers")
             .save())

# COMMAND ----------

(newSalesBuyingGroupsDf.write.format("com.microsoft.cdm")
             .option("entity", "Sales BuyingGroups")
             .option("appId", "bd8e91da-c58c-4407-95e0-a8ecad012fc7")
             .option("appKey", "Lky5pTg2teniHdbeTlmOuivjdIPBPcwMQJ49wGwKiXA=")
             .option("tenantId", "72f988bf-86f1-41af-91ab-2d7cd011db47")
             .option("cdmFolder", "https://cdsabyosadev01dxt.dfs.core.windows.net/powerbi/MatthewTest/wideworldimportersdemo")
             .option("cdmModelName", "Transformed-Wide-World-Importers")
             .save())

# COMMAND ----------

(warehouseStockItemsDf.write.format("com.microsoft.cdm")
             .option("entity", "Warehouse StockItems")
             .option("appId", "bd8e91da-c58c-4407-95e0-a8ecad012fc7")
             .option("appKey", "Lky5pTg2teniHdbeTlmOuivjdIPBPcwMQJ49wGwKiXA=")
             .option("tenantId", "72f988bf-86f1-41af-91ab-2d7cd011db47")
             .option("cdmFolder", "https://cdsabyosadev01dxt.dfs.core.windows.net/powerbi/MatthewTest/wideworldimportersdemo")
             .option("cdmModelName", "Transformed-Wide-World-Importers")
             .save())

# COMMAND ----------

(warehouseColorsDf.write.format("com.microsoft.cdm")
             .option("entity", "Warehouse Colors")
             .option("appId", "bd8e91da-c58c-4407-95e0-a8ecad012fc7")
             .option("appKey", "Lky5pTg2teniHdbeTlmOuivjdIPBPcwMQJ49wGwKiXA=")
             .option("tenantId", "72f988bf-86f1-41af-91ab-2d7cd011db47")
             .option("cdmFolder", "https://cdsabyosadev01dxt.dfs.core.windows.net/powerbi/MatthewTest/wideworldimportersdemo")
             .option("cdmModelName", "Transformed-Wide-World-Importers")
             .save())

# COMMAND ----------

(warehousePackageTypesDf.write.format("com.microsoft.cdm")
             .option("entity", "Warehouse PackageTypes")
             .option("appId", "bd8e91da-c58c-4407-95e0-a8ecad012fc7")
             .option("appKey", "Lky5pTg2teniHdbeTlmOuivjdIPBPcwMQJ49wGwKiXA=")
             .option("tenantId", "72f988bf-86f1-41af-91ab-2d7cd011db47")
             .option("cdmFolder", "https://cdsabyosadev01dxt.dfs.core.windows.net/powerbi/MatthewTest/wideworldimportersdemo")
             .option("cdmModelName", "Transformed-Wide-World-Importers")
             .save())
