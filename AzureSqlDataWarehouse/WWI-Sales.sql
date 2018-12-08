IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'wwi')
EXEC('CREATE SCHEMA [wwi]')
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'wwi')
EXEC('CREATE SCHEMA [wwi]')
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'temp')
EXEC('CREATE SCHEMA [temp]')
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'wwi')
EXEC('CREATE SCHEMA [wwi]')
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'view')
EXEC('CREATE SCHEMA [view]')
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'expired')
EXEC('CREATE SCHEMA [expired]')
GO

-- DIMENSIONAL MODEL TABLES 
IF(OBJECT_ID('wwi.Order')) IS NOT NULL DROP TABLE [wwi].[Order]
CREATE TABLE [wwi].[Order]
(
    [OrderLineID] BIGINT NULL,
    [OrderID] BIGINT NULL,
    [WarehouseStockItemsKey] INT NULL,
    [Description] NVARCHAR(300) NULL,
    [PackageTypeID] BIGINT NULL,
    [Quantity] BIGINT NULL,
    [UnitPrice] DECIMAL(18,4) NULL,
    [TaxRate] DECIMAL(18,4) NULL,
    [PickedQuantity] BIGINT NULL,
    [PickingCompletedWhenKey] INT NULL,
    [LastEditedBy] BIGINT NULL,
    [LastEditedWhen] DATETIME NULL,
    [CustomerID] BIGINT NULL,
    [SalespersonPersonID] BIGINT NULL,
    [PickedByPersonID] BIGINT NULL,
    [ContactPersonID] BIGINT NULL,
    [BackorderOrderID] BIGINT NULL,
    [OrderDateKey] INT NULL,
    [ExpectedDeliveryDateKey] INT NULL,
    [CustomerPurchaseOrderNumber] NVARCHAR(300) NULL,
    [IsUndersupplyBackordered] BIT NULL,
    [Comments] NVARCHAR(300) NULL,
    [DeliveryInstructions] NVARCHAR(300) NULL,
    [InternalComments] NVARCHAR(300) NULL,
    [SalesOrdersPickingCompletedWhenKey] INT NULL,
    [SalesOrdersLastEditedBy] BIGINT NULL,
    [SalesOrdersLastEditedWhen] DATETIME NULL
)
WITH 
(
    CLUSTERED COLUMNSTORE INDEX,
    DISTRIBUTION = ROUND_ROBIN
)

IF(OBJECT_ID('wwi.Customer')) IS NOT NULL DROP TABLE [wwi].[Customer]
CREATE TABLE [wwi].[Customer]
(
    [CustomerKeyPK] INT NOT NULL IDENTITY (1,1),
    [CustomerID] BIGINT NULL,
    [CustomerName] NVARCHAR(300) NULL,
    [BillToCustomerID] BIGINT NULL,
    [CustomerCategoryID] BIGINT NULL,
    [BuyingGroupID] BIGINT NULL,
    [PrimaryContactPersonID] BIGINT NULL,
    [AlternateContactPersonID] BIGINT NULL,
    [DeliveryMethodID] BIGINT NULL,
    [DeliveryCityID] BIGINT NULL,
    [PostalCityID] BIGINT NULL,
    [CreditLimit] DECIMAL(18,4) NULL,
    [AccountOpenedDate] DATETIME NULL,
    [StandardDiscountPercentage] DECIMAL(18,4) NULL,
    [IsStatementSent] BIT NULL,
    [IsOnCreditHold] BIT NULL,
    [PaymentDays] BIGINT NULL,
    [PhoneNumber] NVARCHAR(300) NULL,
    [FaxNumber] NVARCHAR(300) NULL,
    [DeliveryRun] NVARCHAR(300) NULL,
    [RunPosition] NVARCHAR(300) NULL,
    [WebsiteURL] NVARCHAR(300) NULL,
    [DeliveryAddressLine1] NVARCHAR(300) NULL,
    [DeliveryAddressLine2] NVARCHAR(300) NULL,
    [DeliveryPostalCode] NVARCHAR(300) NULL,
    [DeliveryLocation] NVARCHAR(300) NULL,
    [PostalAddressLine1] NVARCHAR(300) NULL,
    [PostalAddressLine2] NVARCHAR(300) NULL,
    [PostalPostalCode] NVARCHAR(300) NULL,
    [LastEditedBy] BIGINT NULL,
    [ValidFrom] DATETIME NULL,
    [ValidTo] DATETIME NULL,
    [CustomerCategoryName] NVARCHAR(300) NULL,
    [SalesCustomerCategoriesLastEditedBy] BIGINT NULL,
    [SalesCustomerCategoriesValidFrom] DATETIME NULL,
    [SalesCustomerCategoriesValidTo] DATETIME NULL,
    [BuyingGroupName] NVARCHAR(300) NULL,
    [SalesBuyingGroupsLastEditedBy] BIGINT NULL,
    [SalesBuyingGroupsValidFrom] DATETIME NULL,
    [SalesBuyingGroupsValidTo] DATETIME NULL
)
WITH 
(
    CLUSTERED COLUMNSTORE INDEX,
    DISTRIBUTION = ROUND_ROBIN
)

IF(OBJECT_ID('wwi.StockItem')) IS NOT NULL DROP TABLE [wwi].[StockItem]
CREATE TABLE [wwi].[StockItem]
(
    [StockItemKeyPK] INT NOT NULL IDENTITY (1,1),
    [StockItemID] BIGINT NULL,
    [StockItemName] NVARCHAR(300) NULL,
    [SupplierID] BIGINT NULL,
    [ColorID] BIGINT NULL,
    [UnitPackageID] BIGINT NULL,
    [OuterPackageID] BIGINT NULL,
    [Brand] NVARCHAR(300) NULL,
    [Size] NVARCHAR(300) NULL,
    [LeadTimeDays] BIGINT NULL,
    [QuantityPerOuter] BIGINT NULL,
    [IsChillerStock] BIT NULL,
    [Barcode] NVARCHAR(300) NULL,
    [TaxRate] DECIMAL(18,4) NULL,
    [UnitPrice] DECIMAL(18,4) NULL,
    [RecommendedRetailPrice] DECIMAL(18,4) NULL,
    [TypicalWeightPerUnit] DECIMAL(18,4) NULL,
    [MarketingComments] NVARCHAR(300) NULL,
    [InternalComments] NVARCHAR(300) NULL,
    [CustomFields] NVARCHAR(300) NULL,
    [Tags] NVARCHAR(300) NULL,
    [SearchDetails] NVARCHAR(300) NULL,
    [LastEditedBy] BIGINT NULL,
    [ValidFrom] DATETIME NULL,
    [ColorName] NVARCHAR(300) NULL,
    [ColorsLastEditedBy] BIGINT NULL,
    [ColorsValidFrom] DATETIME NULL,
    [PackageTypeName] NVARCHAR(300) NULL,
    [UnitPackageTypeLastEditedBy] BIGINT NULL,
    [UnitPackageTypeValidFrom] DATETIME NULL,
    [OuterPackageTypePackageTypeName] NVARCHAR(300) NULL,
    [OuterPackageTypeLastEditedBy] BIGINT NULL,
    [OuterPackageTypeValidFrom] DATETIME NULL
)
WITH 
(
    CLUSTERED COLUMNSTORE INDEX,
    DISTRIBUTION = ROUND_ROBIN
)


IF(OBJECT_ID('wwi.dimDate')) IS NOT NULL DROP TABLE [wwi].[dimDate]
CREATE TABLE [wwi].[dimDate]
WITH (DISTRIBUTION = REPLICATE)
AS
WITH BaseData AS(SELECT A = 0 UNION ALL SELECT A = 1 UNION ALL SELECT A = 2 UNION ALL SELECT A = 3 UNION ALL SELECT A = 4 UNION ALL SELECT A = 5 UNION ALL SELECT A = 6 UNION ALL SELECT A = 7 UNION ALL SELECT A = 8 UNION ALL SELECT A = 9)
,DateSeed AS(SELECT RID = ROW_NUMBER() OVER (ORDER BY A.A) FROM BaseData A CROSS APPLY BaseData B CROSS APPLY BaseData C CROSS APPLY BaseData D CROSS APPLY BaseData E)
,DateBase AS(SELECT TOP 18628 DateValue = cast(DATEADD(D, RID, '1979-12-31')AS DATE) FROM DateSeed)
SELECT DateID = cast(replace(cast(DateValue as varchar(25)), '-', '') as int),
    DateValue = cast(DateValue as date),
    DateYear = DATEPART(year, DateValue),
    DateMonth = DATEPART(month, DateValue),
    DateDay = DATEPART(day, DateValue),
    DateDayOfYear = DATEPART(dayofyear, DateValue),
    DateWeekday = DATEPART(weekday, DateValue),
    DateWeek = DATEPART(week, DateValue),
    DateQuarter = DATEPART(quarter, DateValue),
    DateMonthName = DATENAME(month, DateValue),
    DateQuarterName = 'Q' + DATENAME(quarter, DateValue),
    DateWeekdayName = DATENAME(weekday, DateValue),
    MonthYear = LEFT(DATENAME(month, DateValue), 3) + '-' + DATENAME(year, DateValue)
FROM DateBase;
GO

-- TRANSFORM CODE

-- wwi::SalesOrderLines
-- TRANSFORM VIEW
IF(OBJECT_ID('dbo.tv_SalesOrderLines')) IS NOT NULL DROP VIEW [dbo].[tv_SalesOrderLines]
GO
CREATE VIEW [dbo].[tv_SalesOrderLines]
AS
SELECT
    [SalesOrderLines].[OrderLineID],
    [SalesOrderLines].[OrderID],
    [SalesOrderLines].[StockItemID],
    [SalesOrderLines].[Description],
    [SalesOrderLines].[PackageTypeID],
    [SalesOrderLines].[Quantity],
    [SalesOrderLines].[UnitPrice],
    [SalesOrderLines].[TaxRate],
    [SalesOrderLines].[PickedQuantity],
    [SalesOrderLines].[PickingCompletedWhen],
    [SalesOrderLines].[LastEditedBy],
    [SalesOrderLines].[LastEditedWhen],
    [SalesOrders].[CustomerID],
    [SalesOrders].[SalespersonPersonID],
    [SalesOrders].[PickedByPersonID],
    [SalesOrders].[ContactPersonID],
    [SalesOrders].[BackorderOrderID],
    [SalesOrders].[OrderDate],
    [SalesOrders].[ExpectedDeliveryDate],
    [SalesOrders].[CustomerPurchaseOrderNumber],
    [SalesOrders].[IsUndersupplyBackordered],
    [SalesOrders].[Comments],
    [SalesOrders].[DeliveryInstructions],
    [SalesOrders].[InternalComments],
    [SalesOrders].[PickingCompletedWhen] AS [SalesOrdersPickingCompletedWhen],
    [SalesOrders].[LastEditedBy] AS [SalesOrdersLastEditedBy],
    [SalesOrders].[LastEditedWhen] AS [SalesOrdersLastEditedWhen]
FROM [dbo].[SalesOrderLines] AS [SalesOrderLines]
LEFT OUTER JOIN [dbo].[SalesOrders] AS [SalesOrders] ON [SalesOrderLines].[OrderID] = [SalesOrders].[OrderID]
GO

-- TRANSFORM TABLE
IF(OBJECT_ID('dbo.tt_SalesOrderLines')) IS NOT NULL DROP TABLE [dbo].[tt_SalesOrderLines]

CREATE TABLE [dbo].[tt_SalesOrderLines]
WITH (DISTRIBUTION = ROUND_ROBIN)
AS
SELECT
    [OrderLineID],
    [OrderID],
    [StockItemID],
    [Description],
    [PackageTypeID],
    [Quantity],
    [UnitPrice],
    [TaxRate],
    [PickedQuantity],
    [PickingCompletedWhen],
    [LastEditedBy],
    [LastEditedWhen],
    [CustomerID],
    [SalespersonPersonID],
    [PickedByPersonID],
    [ContactPersonID],
    [BackorderOrderID],
    [OrderDate],
    [ExpectedDeliveryDate],
    [CustomerPurchaseOrderNumber],
    [IsUndersupplyBackordered],
    [Comments],
    [DeliveryInstructions],
    [InternalComments],
    [SalesOrdersPickingCompletedWhen],
    [SalesOrdersLastEditedBy],
    [SalesOrdersLastEditedWhen]
FROM [dbo].[tv_SalesOrderLines] AS [SalesOrderLines]
GO

-- TRANSFORM TABLE VIEW
IF(OBJECT_ID('dbo.ttv_SalesOrderLines')) IS NOT NULL DROP VIEW [dbo].[ttv_SalesOrderLines]
GO

CREATE VIEW [dbo].[ttv_SalesOrderLines]
AS 
SELECT
    [TransformTable].[OrderLineID],
    [TransformTable].[OrderID],
    [TransformTable].[Description],
    [TransformTable].[PackageTypeID],
    [TransformTable].[Quantity],
    [TransformTable].[UnitPrice],
    [TransformTable].[TaxRate],
    [TransformTable].[PickedQuantity],
    [PickingCompletedWhenKey] = cast(replace(cast(CONVERT(date, [TransformTable].[PickingCompletedWhen]) as varchar(25)),'-','') as int),
    [TransformTable].[LastEditedBy],
    [TransformTable].[LastEditedWhen],
    [TransformTable].[CustomerID],
    [TransformTable].[SalespersonPersonID],
    [TransformTable].[PickedByPersonID],
    [TransformTable].[ContactPersonID],
    [TransformTable].[BackorderOrderID],
    [OrderDateKey] = cast(replace(cast(CONVERT(date, [TransformTable].[OrderDate]) as varchar(25)),'-','') as int),
    [ExpectedDeliveryDateKey] = cast(replace(cast(CONVERT(date, [TransformTable].[ExpectedDeliveryDate]) as varchar(25)),'-','') as int),
    [TransformTable].[CustomerPurchaseOrderNumber],
    [TransformTable].[IsUndersupplyBackordered],
    [TransformTable].[Comments],
    [TransformTable].[DeliveryInstructions],
    [TransformTable].[InternalComments],
    [SalesOrdersPickingCompletedWhenKey] = cast(replace(cast(CONVERT(date, [TransformTable].[SalesOrdersPickingCompletedWhen]) as varchar(25)),'-','') as int),
    [TransformTable].[SalesOrdersLastEditedBy],
    [TransformTable].[SalesOrdersLastEditedWhen],
    [StockItem].[StockItemKeyPK] AS [WarehouseStockItemsKey]
FROM [dbo].[tt_SalesOrderLines] AS [TransformTable]
INNER JOIN [wwi].[StockItem] AS [StockItem] ON [StockItem].[StockItemID] = [TransformTable].[StockItemID]
GO

-- TRANSFORM PROCEDURE
IF(OBJECT_ID('dbo.usp_transform_Order')) IS NOT NULL DROP PROCEDURE [dbo].[usp_transform_Order]
GO
-- wwi.Order Upsert In Place
CREATE PROCEDURE [dbo].[usp_transform_Order]
AS
BEGIN
    -- Create materialized transform table from the transform view.
    IF(OBJECT_ID('dbo.tt_SalesOrderLines')) IS NOT NULL DROP TABLE [dbo].[tt_SalesOrderLines]

    CREATE TABLE [dbo].[tt_SalesOrderLines]
    WITH (DISTRIBUTION = ROUND_ROBIN)
    AS
    SELECT
        [OrderLineID],
        [OrderID],
        [StockItemID],
        [Description],
        [PackageTypeID],
        [Quantity],
        [UnitPrice],
        [TaxRate],
        [PickedQuantity],
        [PickingCompletedWhen],
        [LastEditedBy],
        [LastEditedWhen],
        [CustomerID],
        [SalespersonPersonID],
        [PickedByPersonID],
        [ContactPersonID],
        [BackorderOrderID],
        [OrderDate],
        [ExpectedDeliveryDate],
        [CustomerPurchaseOrderNumber],
        [IsUndersupplyBackordered],
        [Comments],
        [DeliveryInstructions],
        [InternalComments],
        [SalesOrdersPickingCompletedWhen],
        [SalesOrdersLastEditedBy],
        [SalesOrdersLastEditedWhen]
    FROM [dbo].[tv_SalesOrderLines] AS [SalesOrderLines]

    -- Insert new rows into the target table from the transform table view, which substitutes surrogate keys and date keys.
    INSERT INTO [wwi].[Order]
    (
        [OrderLineID],
        [OrderID],
        [Description],
        [PackageTypeID],
        [Quantity],
        [UnitPrice],
        [TaxRate],
        [PickedQuantity],
        [PickingCompletedWhenKey],
        [LastEditedBy],
        [LastEditedWhen],
        [CustomerID],
        [SalespersonPersonID],
        [PickedByPersonID],
        [ContactPersonID],
        [BackorderOrderID],
        [OrderDateKey],
        [ExpectedDeliveryDateKey],
        [CustomerPurchaseOrderNumber],
        [IsUndersupplyBackordered],
        [Comments],
        [DeliveryInstructions],
        [InternalComments],
        [SalesOrdersPickingCompletedWhenKey],
        [SalesOrdersLastEditedBy],
        [SalesOrdersLastEditedWhen],
        [WarehouseStockItemsKey]
    )
    SELECT
        [TransformTableView].[OrderLineID],
        [TransformTableView].[OrderID],
        [TransformTableView].[Description],
        [TransformTableView].[PackageTypeID],
        [TransformTableView].[Quantity],
        [TransformTableView].[UnitPrice],
        [TransformTableView].[TaxRate],
        [TransformTableView].[PickedQuantity],
        [TransformTableView].[PickingCompletedWhenKey],
        [TransformTableView].[LastEditedBy],
        [TransformTableView].[LastEditedWhen],
        [TransformTableView].[CustomerID],
        [TransformTableView].[SalespersonPersonID],
        [TransformTableView].[PickedByPersonID],
        [TransformTableView].[ContactPersonID],
        [TransformTableView].[BackorderOrderID],
        [TransformTableView].[OrderDateKey],
        [TransformTableView].[ExpectedDeliveryDateKey],
        [TransformTableView].[CustomerPurchaseOrderNumber],
        [TransformTableView].[IsUndersupplyBackordered],
        [TransformTableView].[Comments],
        [TransformTableView].[DeliveryInstructions],
        [TransformTableView].[InternalComments],
        [TransformTableView].[SalesOrdersPickingCompletedWhenKey],
        [TransformTableView].[SalesOrdersLastEditedBy],
        [TransformTableView].[SalesOrdersLastEditedWhen],
        [TransformTableView].[WarehouseStockItemsKey]
    FROM [dbo].[ttv_SalesOrderLines] AS [TransformTableView]
    WHERE NOT EXISTS
    (SELECT 1 FROM [wwi].[Order] AS [Order]
    WHERE [TransformTableView].[OrderLineID] = [Order].[OrderLineID])

    -- Update modified rows in the transform table view to the target table.
    UPDATE [wwi].[Order]
    SET
        [OrderLineID] = [TransformTableView].[OrderLineID],
        [OrderID] = [TransformTableView].[OrderID],
        [Description] = [TransformTableView].[Description],
        [PackageTypeID] = [TransformTableView].[PackageTypeID],
        [Quantity] = [TransformTableView].[Quantity],
        [UnitPrice] = [TransformTableView].[UnitPrice],
        [TaxRate] = [TransformTableView].[TaxRate],
        [PickedQuantity] = [TransformTableView].[PickedQuantity],
        [PickingCompletedWhenKey] = [TransformTableView].[PickingCompletedWhenKey],
        [LastEditedBy] = [TransformTableView].[LastEditedBy],
        [LastEditedWhen] = [TransformTableView].[LastEditedWhen],
        [CustomerID] = [TransformTableView].[CustomerID],
        [SalespersonPersonID] = [TransformTableView].[SalespersonPersonID],
        [PickedByPersonID] = [TransformTableView].[PickedByPersonID],
        [ContactPersonID] = [TransformTableView].[ContactPersonID],
        [BackorderOrderID] = [TransformTableView].[BackorderOrderID],
        [OrderDateKey] = [TransformTableView].[OrderDateKey],
        [ExpectedDeliveryDateKey] = [TransformTableView].[ExpectedDeliveryDateKey],
        [CustomerPurchaseOrderNumber] = [TransformTableView].[CustomerPurchaseOrderNumber],
        [IsUndersupplyBackordered] = [TransformTableView].[IsUndersupplyBackordered],
        [Comments] = [TransformTableView].[Comments],
        [DeliveryInstructions] = [TransformTableView].[DeliveryInstructions],
        [InternalComments] = [TransformTableView].[InternalComments],
        [SalesOrdersPickingCompletedWhenKey] = [TransformTableView].[SalesOrdersPickingCompletedWhenKey],
        [SalesOrdersLastEditedBy] = [TransformTableView].[SalesOrdersLastEditedBy],
        [SalesOrdersLastEditedWhen] = [TransformTableView].[SalesOrdersLastEditedWhen],
        [WarehouseStockItemsKey] = [TransformTableView].[WarehouseStockItemsKey]
    FROM [dbo].[ttv_SalesOrderLines] AS [TransformTableView]
    WHERE 
        [Order].[OrderLineID] = [TransformTableView].[OrderLineID]
        AND 
        ([Order].[LastEditedWhen] != [TransformTableView].[LastEditedWhen]
        OR [Order].[SalesOrdersLastEditedWhen] != [TransformTableView].[SalesOrdersLastEditedWhen])
END
GO


-- wwi::SalesCustomers
-- TRANSFORM VIEW
IF(OBJECT_ID('dbo.tv_SalesCustomers')) IS NOT NULL DROP VIEW [dbo].[tv_SalesCustomers]
GO
CREATE VIEW [dbo].[tv_SalesCustomers]
AS
SELECT
    [SalesCustomers].[CustomerID],
    [SalesCustomers].[CustomerName],
    [SalesCustomers].[BillToCustomerID],
    [SalesCustomers].[CustomerCategoryID],
    [SalesCustomers].[BuyingGroupID],
    [SalesCustomers].[PrimaryContactPersonID],
    [SalesCustomers].[AlternateContactPersonID],
    [SalesCustomers].[DeliveryMethodID],
    [SalesCustomers].[DeliveryCityID],
    [SalesCustomers].[PostalCityID],
    [SalesCustomers].[CreditLimit],
    [SalesCustomers].[AccountOpenedDate],
    [SalesCustomers].[StandardDiscountPercentage],
    [SalesCustomers].[IsStatementSent],
    [SalesCustomers].[IsOnCreditHold],
    [SalesCustomers].[PaymentDays],
    [SalesCustomers].[PhoneNumber],
    [SalesCustomers].[FaxNumber],
    [SalesCustomers].[DeliveryRun],
    [SalesCustomers].[RunPosition],
    [SalesCustomers].[WebsiteURL],
    [SalesCustomers].[DeliveryAddressLine1],
    [SalesCustomers].[DeliveryAddressLine2],
    [SalesCustomers].[DeliveryPostalCode],
    [SalesCustomers].[DeliveryLocation],
    [SalesCustomers].[PostalAddressLine1],
    [SalesCustomers].[PostalAddressLine2],
    [SalesCustomers].[PostalPostalCode],
    [SalesCustomers].[LastEditedBy],
    [SalesCustomers].[ValidFrom],
    [SalesCustomers].[ValidTo],
    [SalesCustomerCategories].[CustomerCategoryName],
    [SalesCustomerCategories].[LastEditedBy] AS [SalesCustomerCategoriesLastEditedBy],
    [SalesCustomerCategories].[ValidFrom] AS [SalesCustomerCategoriesValidFrom],
    [SalesCustomerCategories].[ValidTo] AS [SalesCustomerCategoriesValidTo],
    [SalesBuyingGroups].[BuyingGroupName],
    [SalesBuyingGroups].[LastEditedBy] AS [SalesBuyingGroupsLastEditedBy],
    [SalesBuyingGroups].[ValidFrom] AS [SalesBuyingGroupsValidFrom],
    [SalesBuyingGroups].[ValidTo] AS [SalesBuyingGroupsValidTo]
FROM [dbo].[SalesCustomers] AS [SalesCustomers]
LEFT OUTER JOIN [dbo].[SalesCustomerCategories] AS [SalesCustomerCategories] ON [SalesCustomers].[CustomerCategoryID] = [SalesCustomerCategories].[CustomerCategoryID]
LEFT OUTER JOIN [dbo].[SalesBuyingGroups] AS [SalesBuyingGroups] ON [SalesCustomers].[BuyingGroupID] = [SalesBuyingGroups].[BuyingGroupID]
GO

-- TRANSFORM TABLE
IF(OBJECT_ID('dbo.tt_SalesCustomers')) IS NOT NULL DROP TABLE [dbo].[tt_SalesCustomers]

CREATE TABLE [dbo].[tt_SalesCustomers]
WITH (DISTRIBUTION = ROUND_ROBIN)
AS
SELECT
    [CustomerID],
    [CustomerName],
    [BillToCustomerID],
    [CustomerCategoryID],
    [BuyingGroupID],
    [PrimaryContactPersonID],
    [AlternateContactPersonID],
    [DeliveryMethodID],
    [DeliveryCityID],
    [PostalCityID],
    [CreditLimit],
    [AccountOpenedDate],
    [StandardDiscountPercentage],
    [IsStatementSent],
    [IsOnCreditHold],
    [PaymentDays],
    [PhoneNumber],
    [FaxNumber],
    [DeliveryRun],
    [RunPosition],
    [WebsiteURL],
    [DeliveryAddressLine1],
    [DeliveryAddressLine2],
    [DeliveryPostalCode],
    [DeliveryLocation],
    [PostalAddressLine1],
    [PostalAddressLine2],
    [PostalPostalCode],
    [LastEditedBy],
    [ValidFrom],
    [ValidTo],
    [CustomerCategoryName],
    [SalesCustomerCategoriesLastEditedBy],
    [SalesCustomerCategoriesValidFrom],
    [SalesCustomerCategoriesValidTo],
    [BuyingGroupName],
    [SalesBuyingGroupsLastEditedBy],
    [SalesBuyingGroupsValidFrom],
    [SalesBuyingGroupsValidTo]
FROM [dbo].[tv_SalesCustomers] AS [SalesCustomers]
GO

-- TRANSFORM PROCEDURE
IF(OBJECT_ID('dbo.usp_transform_Customer')) IS NOT NULL DROP PROCEDURE [dbo].[usp_transform_Customer]
GO

-- wwi.Customer Upsert Via Temp Table
CREATE PROCEDURE [dbo].[usp_transform_Customer]
AS
BEGIN
    -- Use CTAS to create transform table with staged data from the transform view.
    IF(OBJECT_ID('dbo.tt_SalesCustomers')) IS NOT NULL DROP TABLE [dbo].[tt_SalesCustomers]

    CREATE TABLE [dbo].[tt_SalesCustomers]
    WITH (DISTRIBUTION = ROUND_ROBIN)
    AS
    SELECT
        [CustomerID],
        [CustomerName],
        [BillToCustomerID],
        [CustomerCategoryID],
        [BuyingGroupID],
        [PrimaryContactPersonID],
        [AlternateContactPersonID],
        [DeliveryMethodID],
        [DeliveryCityID],
        [PostalCityID],
        [CreditLimit],
        [AccountOpenedDate],
        [StandardDiscountPercentage],
        [IsStatementSent],
        [IsOnCreditHold],
        [PaymentDays],
        [PhoneNumber],
        [FaxNumber],
        [DeliveryRun],
        [RunPosition],
        [WebsiteURL],
        [DeliveryAddressLine1],
        [DeliveryAddressLine2],
        [DeliveryPostalCode],
        [DeliveryLocation],
        [PostalAddressLine1],
        [PostalAddressLine2],
        [PostalPostalCode],
        [LastEditedBy],
        [ValidFrom],
        [ValidTo],
        [CustomerCategoryName],
        [SalesCustomerCategoriesLastEditedBy],
        [SalesCustomerCategoriesValidFrom],
        [SalesCustomerCategoriesValidTo],
        [BuyingGroupName],
        [SalesBuyingGroupsLastEditedBy],
        [SalesBuyingGroupsValidFrom],
        [SalesBuyingGroupsValidTo]
    FROM [dbo].[tv_SalesCustomers] AS [SalesCustomers]

    -- Use CTAS to create temporary table to merge existing and new/updated rows

    IF(OBJECT_ID('temp.Customer_temp')) IS NOT NULL DROP TABLE [temp].[Customer_temp]

    CREATE TABLE [temp].[Customer_temp]
    WITH (DISTRIBUTION = ROUND_ROBIN)
    AS
    -- Get rows from target table not present or unchanged in the transform table
    SELECT
        [CustomerKeyPK],
        [CustomerID],
        [CustomerName],
        [BillToCustomerID],
        [CustomerCategoryID],
        [BuyingGroupID],
        [PrimaryContactPersonID],
        [AlternateContactPersonID],
        [DeliveryMethodID],
        [DeliveryCityID],
        [PostalCityID],
        [CreditLimit],
        [AccountOpenedDate],
        [StandardDiscountPercentage],
        [IsStatementSent],
        [IsOnCreditHold],
        [PaymentDays],
        [PhoneNumber],
        [FaxNumber],
        [DeliveryRun],
        [RunPosition],
        [WebsiteURL],
        [DeliveryAddressLine1],
        [DeliveryAddressLine2],
        [DeliveryPostalCode],
        [DeliveryLocation],
        [PostalAddressLine1],
        [PostalAddressLine2],
        [PostalPostalCode],
        [LastEditedBy],
        [ValidFrom],
        [ValidTo],
        [CustomerCategoryName],
        [SalesCustomerCategoriesLastEditedBy],
        [SalesCustomerCategoriesValidFrom],
        [SalesCustomerCategoriesValidTo],
        [BuyingGroupName],
        [SalesBuyingGroupsLastEditedBy],
        [SalesBuyingGroupsValidFrom],
        [SalesBuyingGroupsValidTo]
    FROM [wwi].[Customer] AS [Customer]
    WHERE
        NOT EXISTS -- not in staging 
            (SELECT 1 FROM [dbo].[tt_SalesCustomers] AS [TransformTable]
            WHERE [Customer].[CustomerID] = [TransformTable].[CustomerID])
        OR
        EXISTS -- in staging but not changed
            (SELECT 1 FROM [dbo].[tt_SalesCustomers] AS [TransformTable]
            WHERE [Customer].[CustomerID] = [TransformTable].[CustomerID]
            AND ([Customer].[ValidFrom] = [TransformTable].[ValidFrom]
                AND [Customer].[SalesCustomerCategoriesValidFrom] = [TransformTable].[SalesCustomerCategoriesValidFrom]
                AND [Customer].[SalesBuyingGroupsValidFrom] = [TransformTable].[SalesBuyingGroupsValidFrom]))

    SET IDENTITY_INSERT [temp].[Customer_temp] ON

    -- Insert modified rows 
    INSERT INTO [temp].[Customer_temp]
    (
        [CustomerKeyPK],
        [CustomerID],
        [CustomerName],
        [BillToCustomerID],
        [CustomerCategoryID],
        [BuyingGroupID],
        [PrimaryContactPersonID],
        [AlternateContactPersonID],
        [DeliveryMethodID],
        [DeliveryCityID],
        [PostalCityID],
        [CreditLimit],
        [AccountOpenedDate],
        [StandardDiscountPercentage],
        [IsStatementSent],
        [IsOnCreditHold],
        [PaymentDays],
        [PhoneNumber],
        [FaxNumber],
        [DeliveryRun],
        [RunPosition],
        [WebsiteURL],
        [DeliveryAddressLine1],
        [DeliveryAddressLine2],
        [DeliveryPostalCode],
        [DeliveryLocation],
        [PostalAddressLine1],
        [PostalAddressLine2],
        [PostalPostalCode],
        [LastEditedBy],
        [ValidFrom],
        [ValidTo],
        [CustomerCategoryName],
        [SalesCustomerCategoriesLastEditedBy],
        [SalesCustomerCategoriesValidFrom],
        [SalesCustomerCategoriesValidTo],
        [BuyingGroupName],
        [SalesBuyingGroupsLastEditedBy],
        [SalesBuyingGroupsValidFrom],
        [SalesBuyingGroupsValidTo]
    )
    SELECT
        [Customer].[CustomerKeyPK],
        [TransformTable].[CustomerID],
        [TransformTable].[CustomerName],
        [TransformTable].[BillToCustomerID],
        [TransformTable].[CustomerCategoryID],
        [TransformTable].[BuyingGroupID],
        [TransformTable].[PrimaryContactPersonID],
        [TransformTable].[AlternateContactPersonID],
        [TransformTable].[DeliveryMethodID],
        [TransformTable].[DeliveryCityID],
        [TransformTable].[PostalCityID],
        [TransformTable].[CreditLimit],
        [TransformTable].[AccountOpenedDate],
        [TransformTable].[StandardDiscountPercentage],
        [TransformTable].[IsStatementSent],
        [TransformTable].[IsOnCreditHold],
        [TransformTable].[PaymentDays],
        [TransformTable].[PhoneNumber],
        [TransformTable].[FaxNumber],
        [TransformTable].[DeliveryRun],
        [TransformTable].[RunPosition],
        [TransformTable].[WebsiteURL],
        [TransformTable].[DeliveryAddressLine1],
        [TransformTable].[DeliveryAddressLine2],
        [TransformTable].[DeliveryPostalCode],
        [TransformTable].[DeliveryLocation],
        [TransformTable].[PostalAddressLine1],
        [TransformTable].[PostalAddressLine2],
        [TransformTable].[PostalPostalCode],
        [TransformTable].[LastEditedBy],
        [TransformTable].[ValidFrom],
        [TransformTable].[ValidTo],
        [TransformTable].[CustomerCategoryName],
        [TransformTable].[SalesCustomerCategoriesLastEditedBy],
        [TransformTable].[SalesCustomerCategoriesValidFrom],
        [TransformTable].[SalesCustomerCategoriesValidTo],
        [TransformTable].[BuyingGroupName],
        [TransformTable].[SalesBuyingGroupsLastEditedBy],
        [TransformTable].[SalesBuyingGroupsValidFrom],
        [TransformTable].[SalesBuyingGroupsValidTo]
    FROM [dbo].[tt_SalesCustomers] AS [TransformTable]
    INNER JOIN [wwi].[Customer] AS [Customer] ON [Customer].[CustomerID] = [TransformTable].[CustomerID]
    WHERE ([Customer].[ValidFrom] != [TransformTable].[ValidFrom]
        OR [Customer].[SalesCustomerCategoriesValidFrom] != [TransformTable].[SalesCustomerCategoriesValidFrom]
        OR [Customer].[SalesBuyingGroupsValidFrom] != [TransformTable].[SalesBuyingGroupsValidFrom])

    SET IDENTITY_INSERT [temp].[Customer_temp] OFF

    -- insert the new rows from the staging transform table
    INSERT INTO [temp].[Customer_temp]
    SELECT DISTINCT 
        [CustomerID],
        [CustomerName],
        [BillToCustomerID],
        [CustomerCategoryID],
        [BuyingGroupID],
        [PrimaryContactPersonID],
        [AlternateContactPersonID],
        [DeliveryMethodID],
        [DeliveryCityID],
        [PostalCityID],
        [CreditLimit],
        [AccountOpenedDate],
        [StandardDiscountPercentage],
        [IsStatementSent],
        [IsOnCreditHold],
        [PaymentDays],
        [PhoneNumber],
        [FaxNumber],
        [DeliveryRun],
        [RunPosition],
        [WebsiteURL],
        [DeliveryAddressLine1],
        [DeliveryAddressLine2],
        [DeliveryPostalCode],
        [DeliveryLocation],
        [PostalAddressLine1],
        [PostalAddressLine2],
        [PostalPostalCode],
        [LastEditedBy],
        [ValidFrom],
        [ValidTo],
        [CustomerCategoryName],
        [SalesCustomerCategoriesLastEditedBy],
        [SalesCustomerCategoriesValidFrom],
        [SalesCustomerCategoriesValidTo],
        [BuyingGroupName],
        [SalesBuyingGroupsLastEditedBy],
        [SalesBuyingGroupsValidFrom],
        [SalesBuyingGroupsValidTo]
    FROM [dbo].[tt_SalesCustomers] AS [TransformTable]
    WHERE NOT EXISTS
        (SELECT 1 FROM [wwi].[Customer] AS [Customer]
        WHERE [Customer].[CustomerID] = [TransformTable].[CustomerID])

    -- Delete the old dimension table if it exists (from a prior transform).
    IF OBJECT_ID('expired.Customer_old') IS NOT NULL DROP TABLE [expired].[Customer_old]

    -- Rename the current dimension to old and the temporary table to current. 
    RENAME OBJECT [wwi].[Customer] TO [Customer_old]
    ALTER SCHEMA [expired] TRANSFER [wwi].[Customer_old]

    RENAME OBJECT [temp].[Customer_temp] TO [Customer]
    ALTER SCHEMA [wwi] TRANSFER [temp].[Customer]
END
GO


-- wwi::WarehouseStockItems
-- TRANSFORM VIEW
IF(OBJECT_ID('dbo.tv_WarehouseStockItems')) IS NOT NULL DROP VIEW [dbo].[tv_WarehouseStockItems]
GO
CREATE VIEW [dbo].[tv_WarehouseStockItems]
AS
SELECT
    [WarehouseStockItems].[StockItemID],
    [WarehouseStockItems].[StockItemName],
    [WarehouseStockItems].[SupplierID],
    [WarehouseStockItems].[ColorID],
    [WarehouseStockItems].[UnitPackageID],
    [WarehouseStockItems].[OuterPackageID],
    [WarehouseStockItems].[Brand],
    [WarehouseStockItems].[Size],
    [WarehouseStockItems].[LeadTimeDays],
    [WarehouseStockItems].[QuantityPerOuter],
    [WarehouseStockItems].[IsChillerStock],
    [WarehouseStockItems].[Barcode],
    [WarehouseStockItems].[TaxRate],
    [WarehouseStockItems].[UnitPrice],
    [WarehouseStockItems].[RecommendedRetailPrice],
    [WarehouseStockItems].[TypicalWeightPerUnit],
    [WarehouseStockItems].[MarketingComments],
    [WarehouseStockItems].[InternalComments],
    [WarehouseStockItems].[CustomFields],
    [WarehouseStockItems].[Tags],
    [WarehouseStockItems].[SearchDetails],
    [WarehouseStockItems].[LastEditedBy],
    [WarehouseStockItems].[ValidFrom],
    [Colors].[ColorName],
    [Colors].[LastEditedBy] AS [ColorsLastEditedBy],
    [Colors].[ValidFrom] AS [ColorsValidFrom],
    [UnitPackageType].[PackageTypeName],
    [UnitPackageType].[LastEditedBy] AS [UnitPackageTypeLastEditedBy],
    [UnitPackageType].[ValidFrom] AS [UnitPackageTypeValidFrom],
    [OuterPackageType].[PackageTypeName] AS [OuterPackageTypePackageTypeName],
    [OuterPackageType].[LastEditedBy] AS [OuterPackageTypeLastEditedBy],
    [OuterPackageType].[ValidFrom] AS [OuterPackageTypeValidFrom]
FROM [dbo].[WarehouseStockItems] AS [WarehouseStockItems]
LEFT OUTER JOIN [dbo].[WarehouseColors] AS [Colors] ON [WarehouseStockItems].[ColorID] = [Colors].[ColorID]
LEFT OUTER JOIN [dbo].[WarehousePackageTypes] AS [UnitPackageType] ON [WarehouseStockItems].[UnitPackageID] = [UnitPackageType].[PackageTypeID]
LEFT OUTER JOIN [dbo].[WarehousePackageTypes] AS [OuterPackageType] ON [WarehouseStockItems].[OuterPackageID] = [OuterPackageType].[PackageTypeID]
GO

-- TRANSFORM TABLE
IF(OBJECT_ID('dbo.tt_WarehouseStockItems')) IS NOT NULL DROP TABLE [dbo].[tt_WarehouseStockItems]

CREATE TABLE [dbo].[tt_WarehouseStockItems]
WITH (DISTRIBUTION = ROUND_ROBIN)
AS
SELECT
    [StockItemID],
    [StockItemName],
    [SupplierID],
    [ColorID],
    [UnitPackageID],
    [OuterPackageID],
    [Brand],
    [Size],
    [LeadTimeDays],
    [QuantityPerOuter],
    [IsChillerStock],
    [Barcode],
    [TaxRate],
    [UnitPrice],
    [RecommendedRetailPrice],
    [TypicalWeightPerUnit],
    [MarketingComments],
    [InternalComments],
    [CustomFields],
    [Tags],
    [SearchDetails],
    [LastEditedBy],
    [ValidFrom],
    [ColorName],
    [ColorsLastEditedBy],
    [ColorsValidFrom],
    [PackageTypeName],
    [UnitPackageTypeLastEditedBy],
    [UnitPackageTypeValidFrom],
    [OuterPackageTypePackageTypeName],
    [OuterPackageTypeLastEditedBy],
    [OuterPackageTypeValidFrom]
FROM [dbo].[tv_WarehouseStockItems] AS [WarehouseStockItems]
GO

-- TRANSFORM PROCEDURE
IF(OBJECT_ID('dbo.usp_transform_StockItem')) IS NOT NULL DROP PROCEDURE [dbo].[usp_transform_StockItem]
GO

-- wwi.StockItem Upsert Via Temp Table
CREATE PROCEDURE [dbo].[usp_transform_StockItem]
AS
BEGIN
    -- Use CTAS to create transform table with staged data from the transform view.
    IF(OBJECT_ID('dbo.tt_WarehouseStockItems')) IS NOT NULL DROP TABLE [dbo].[tt_WarehouseStockItems]

    CREATE TABLE [dbo].[tt_WarehouseStockItems]
    WITH (DISTRIBUTION = ROUND_ROBIN)
    AS
    SELECT
        [StockItemID],
        [StockItemName],
        [SupplierID],
        [ColorID],
        [UnitPackageID],
        [OuterPackageID],
        [Brand],
        [Size],
        [LeadTimeDays],
        [QuantityPerOuter],
        [IsChillerStock],
        [Barcode],
        [TaxRate],
        [UnitPrice],
        [RecommendedRetailPrice],
        [TypicalWeightPerUnit],
        [MarketingComments],
        [InternalComments],
        [CustomFields],
        [Tags],
        [SearchDetails],
        [LastEditedBy],
        [ValidFrom],
        [ColorName],
        [ColorsLastEditedBy],
        [ColorsValidFrom],
        [PackageTypeName],
        [UnitPackageTypeLastEditedBy],
        [UnitPackageTypeValidFrom],
        [OuterPackageTypePackageTypeName],
        [OuterPackageTypeLastEditedBy],
        [OuterPackageTypeValidFrom]
    FROM [dbo].[tv_WarehouseStockItems] AS [WarehouseStockItems]

    -- Use CTAS to create temporary table to merge existing and new/updated rows

    IF(OBJECT_ID('temp.StockItem_temp')) IS NOT NULL DROP TABLE [temp].[StockItem_temp]

    CREATE TABLE [temp].[StockItem_temp]
    WITH (DISTRIBUTION = ROUND_ROBIN)
    AS
    -- Get rows from target table not present or unchanged in the transform table
    SELECT
        [StockItemKeyPK],
        [StockItemID],
        [StockItemName],
        [SupplierID],
        [ColorID],
        [UnitPackageID],
        [OuterPackageID],
        [Brand],
        [Size],
        [LeadTimeDays],
        [QuantityPerOuter],
        [IsChillerStock],
        [Barcode],
        [TaxRate],
        [UnitPrice],
        [RecommendedRetailPrice],
        [TypicalWeightPerUnit],
        [MarketingComments],
        [InternalComments],
        [CustomFields],
        [Tags],
        [SearchDetails],
        [LastEditedBy],
        [ValidFrom],
        [ColorName],
        [ColorsLastEditedBy],
        [ColorsValidFrom],
        [PackageTypeName],
        [UnitPackageTypeLastEditedBy],
        [UnitPackageTypeValidFrom],
        [OuterPackageTypePackageTypeName],
        [OuterPackageTypeLastEditedBy],
        [OuterPackageTypeValidFrom]
    FROM [wwi].[StockItem] AS [StockItem]
    WHERE
        NOT EXISTS -- not in staging 
            (SELECT 1 FROM [dbo].[tt_WarehouseStockItems] AS [TransformTable]
            WHERE [StockItem].[StockItemID] = [TransformTable].[StockItemID])
        OR
        EXISTS -- in staging but not changed
            (SELECT 1 FROM [dbo].[tt_WarehouseStockItems] AS [TransformTable]
            WHERE [StockItem].[StockItemID] = [TransformTable].[StockItemID]
            AND ([StockItem].[ValidFrom] = [TransformTable].[ValidFrom]
                AND [StockItem].[ColorsValidFrom] = [TransformTable].[ColorsValidFrom]
                AND [StockItem].[UnitPackageTypeValidFrom] = [TransformTable].[UnitPackageTypeValidFrom]
                AND [StockItem].[OuterPackageTypeValidFrom] = [TransformTable].[OuterPackageTypeValidFrom]))

    SET IDENTITY_INSERT [temp].[StockItem_temp] ON

    -- Insert modified rows 
    INSERT INTO [temp].[StockItem_temp]
    (
        [StockItemKeyPK],
        [StockItemID],
        [StockItemName],
        [SupplierID],
        [ColorID],
        [UnitPackageID],
        [OuterPackageID],
        [Brand],
        [Size],
        [LeadTimeDays],
        [QuantityPerOuter],
        [IsChillerStock],
        [Barcode],
        [TaxRate],
        [UnitPrice],
        [RecommendedRetailPrice],
        [TypicalWeightPerUnit],
        [MarketingComments],
        [InternalComments],
        [CustomFields],
        [Tags],
        [SearchDetails],
        [LastEditedBy],
        [ValidFrom],
        [ColorName],
        [ColorsLastEditedBy],
        [ColorsValidFrom],
        [PackageTypeName],
        [UnitPackageTypeLastEditedBy],
        [UnitPackageTypeValidFrom],
        [OuterPackageTypePackageTypeName],
        [OuterPackageTypeLastEditedBy],
        [OuterPackageTypeValidFrom]
    )
    SELECT
        [StockItem].[StockItemKeyPK],
        [TransformTable].[StockItemID],
        [TransformTable].[StockItemName],
        [TransformTable].[SupplierID],
        [TransformTable].[ColorID],
        [TransformTable].[UnitPackageID],
        [TransformTable].[OuterPackageID],
        [TransformTable].[Brand],
        [TransformTable].[Size],
        [TransformTable].[LeadTimeDays],
        [TransformTable].[QuantityPerOuter],
        [TransformTable].[IsChillerStock],
        [TransformTable].[Barcode],
        [TransformTable].[TaxRate],
        [TransformTable].[UnitPrice],
        [TransformTable].[RecommendedRetailPrice],
        [TransformTable].[TypicalWeightPerUnit],
        [TransformTable].[MarketingComments],
        [TransformTable].[InternalComments],
        [TransformTable].[CustomFields],
        [TransformTable].[Tags],
        [TransformTable].[SearchDetails],
        [TransformTable].[LastEditedBy],
        [TransformTable].[ValidFrom],
        [TransformTable].[ColorName],
        [TransformTable].[ColorsLastEditedBy],
        [TransformTable].[ColorsValidFrom],
        [TransformTable].[PackageTypeName],
        [TransformTable].[UnitPackageTypeLastEditedBy],
        [TransformTable].[UnitPackageTypeValidFrom],
        [TransformTable].[OuterPackageTypePackageTypeName],
        [TransformTable].[OuterPackageTypeLastEditedBy],
        [TransformTable].[OuterPackageTypeValidFrom]
    FROM [dbo].[tt_WarehouseStockItems] AS [TransformTable]
    INNER JOIN [wwi].[StockItem] AS [StockItem] ON [StockItem].[StockItemID] = [TransformTable].[StockItemID]
    WHERE ([StockItem].[ValidFrom] != [TransformTable].[ValidFrom]
        OR [StockItem].[ColorsValidFrom] != [TransformTable].[ColorsValidFrom]
        OR [StockItem].[UnitPackageTypeValidFrom] != [TransformTable].[UnitPackageTypeValidFrom]
        OR [StockItem].[OuterPackageTypeValidFrom] != [TransformTable].[OuterPackageTypeValidFrom])

    SET IDENTITY_INSERT [temp].[StockItem_temp] OFF

    -- insert the new rows from the staging transform table
    INSERT INTO [temp].[StockItem_temp]
    SELECT DISTINCT 
        [StockItemID],
        [StockItemName],
        [SupplierID],
        [ColorID],
        [UnitPackageID],
        [OuterPackageID],
        [Brand],
        [Size],
        [LeadTimeDays],
        [QuantityPerOuter],
        [IsChillerStock],
        [Barcode],
        [TaxRate],
        [UnitPrice],
        [RecommendedRetailPrice],
        [TypicalWeightPerUnit],
        [MarketingComments],
        [InternalComments],
        [CustomFields],
        [Tags],
        [SearchDetails],
        [LastEditedBy],
        [ValidFrom],
        [ColorName],
        [ColorsLastEditedBy],
        [ColorsValidFrom],
        [PackageTypeName],
        [UnitPackageTypeLastEditedBy],
        [UnitPackageTypeValidFrom],
        [OuterPackageTypePackageTypeName],
        [OuterPackageTypeLastEditedBy],
        [OuterPackageTypeValidFrom]
    FROM [dbo].[tt_WarehouseStockItems] AS [TransformTable]
    WHERE NOT EXISTS
        (SELECT 1 FROM [wwi].[StockItem] AS [StockItem]
        WHERE [StockItem].[StockItemID] = [TransformTable].[StockItemID])

    -- Delete the old dimension table if it exists (from a prior transform).
    IF OBJECT_ID('expired.StockItem_old') IS NOT NULL DROP TABLE [expired].[StockItem_old]

    -- Rename the current dimension to old and the temporary table to current. 
    RENAME OBJECT [wwi].[StockItem] TO [StockItem_old]
    ALTER SCHEMA [expired] TRANSFER [wwi].[StockItem_old]

    RENAME OBJECT [temp].[StockItem_temp] TO [StockItem]
    ALTER SCHEMA [wwi] TRANSFER [temp].[StockItem]
END
GO

-- TRANSFORM CONTROLLER
IF(OBJECT_ID('dbo.uspTransformWideWorldImporters-Sales')) IS NOT NULL DROP PROCEDURE [dbo].[uspTransformWideWorldImporters-Sales]
GO

-- WideWorldImporters-Sales model transform controller
CREATE PROCEDURE [dbo].[uspTransformWideWorldImporters-Sales]
AS
BEGIN
-- Transform dimension tables
    EXEC [dbo].[usp_transform_Customer]
    EXEC [dbo].[usp_transform_StockItem]
-- Transform fact tables
    EXEC [dbo].[usp_transform_Order]
END


/*
Transform Controller, transform all dimensional tables from staging tables in correct order
EXEC [dbo].[uspTransformWideWorldImporters-Sales]

Query dimensional model tables mapped from entity model WideWorldImporters-Sales
SELECT TOP 100 * FROM [wwi].[Order]
SELECT TOP 100 * FROM [wwi].[Customer]
SELECT TOP 100 * FROM [wwi].[StockItem]
*/