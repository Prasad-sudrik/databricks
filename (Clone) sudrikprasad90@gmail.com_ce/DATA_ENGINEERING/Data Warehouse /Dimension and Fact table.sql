-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.fs.ls("dbfs:/FileStore")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/",True)

-- COMMAND ----------

DROP DATABASE sales_scd;

-- COMMAND ----------

CREATE DATABASE sales_scd;

-- COMMAND ----------

CREATE OR REPLACE TABLE sales_scd.Orders (
    OrderID INT,
    OrderDate DATE,
    CustomerID INT,
    CustomerName VARCHAR(100),
    CustomerEmail VARCHAR(100),
    ProductID INT,
    ProductName VARCHAR(100),
    ProductCategory VARCHAR(50),
    RegionID INT,
    RegionName VARCHAR(50),
    Country VARCHAR(50),
    Quantity INT,
    UnitPrice DECIMAL(10,2),
    TotalAmount DECIMAL(10,2)
);

-- COMMAND ----------

INSERT INTO sales_scd.Orders (OrderID, OrderDate, CustomerID, CustomerName, CustomerEmail, ProductID, ProductName, ProductCategory, RegionID, RegionName, Country, Quantity, UnitPrice, TotalAmount) 
VALUES 
(1, '2024-02-01', 101, 'Alice Johnson', 'alice@example.com', 201, 'Laptop', 'Electronics', 301, 'North America', 'USA', 2, 800.00, 1600.00),
(2, '2024-02-02', 102, 'Bob Smith', 'bob@example.com', 202, 'Smartphone', 'Electronics', 302, 'Europe', 'Germany', 1, 500.00, 500.00),
(3, '2024-02-03', 103, 'Charlie Brown', 'charlie@example.com', 203, 'Tablet', 'Electronics', 303, 'Asia', 'India', 3, 300.00, 900.00),
(4, '2024-02-04', 101, 'Alice Johnson', 'alice@example.com', 204, 'Headphones', 'Accessories', 301, 'North America', 'USA', 1, 150.00, 150.00),
(5, '2024-02-05', 104, 'David Lee', 'david@example.com', 205, 'Gaming Console', 'Electronics', 302, 'Europe', 'France', 1, 400.00, 400.00),
(6, '2024-02-06', 102, 'Bob Smith', 'bob@example.com', 206, 'Smartwatch', 'Electronics', 303, 'Asia', 'China', 2, 200.00, 400.00),
(7, '2024-02-07', 105, 'Eve Adams', 'eve@example.com', 201, 'Laptop', 'Electronics', 301, 'North America', 'Canada', 1, 800.00, 800.00),
(8, '2024-02-08', 106, 'Frank Miller', 'frank@example.com', 207, 'Monitor', 'Accessories', 302, 'Europe', 'Italy', 2, 250.00, 500.00),
(9, '2024-02-09', 107, 'Grace White', 'grace@example.com', 208, 'Keyboard', 'Accessories', 303, 'Asia', 'Japan', 3, 100.00, 300.00),
(10, '2024-02-10', 104, 'David Lee', 'david@example.com', 209, 'Mouse', 'Accessories', 301, 'North America', 'USA', 1, 50.00, 50.00);

-- COMMAND ----------

select * from sales_scd.orders

-- COMMAND ----------

CREATE DATABASE ordersDWH

-- COMMAND ----------

CREATE TABLE ordersDWH.stg_sales
AS
SELECT * FROM sales_scd.orders;

-- COMMAND ----------

CREATE VIEW ordersDWH.trans_sales AS
SELECT * FROM ordersDWH.stg_sales WHERE Quantity IS NOT NULL

-- COMMAND ----------

SELECT * FROM ordersDWH.trans_sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC DimCustomers

-- COMMAND ----------

CREATE OR REPLACE TABLE ordersDWH.DimCustomers
(
  CustomerID INT,
  CustomerName STRING,
  CustomerEmail STRING,
  DimCustomersKey INT
)

-- COMMAND ----------

CREATE OR REPLACE VIEW ordersDWH.view_DimCustomers
AS
SELECT T.*,row_number() OVER (ORDER BY T.CustomerID) as DimCustomersKey
FROM
( 
SELECT 
  DISTINCT (CustomerID) as CustomerID,
  CustomerName,
  CustomerEmail
FROM
  ordersDWH.trans_sales
) AS T


-- COMMAND ----------

SELECT * FROM ordersDWH.view_DimCustomers;

-- COMMAND ----------

INSERT INTO ordersdwh.DimCustomers
SELECT * FROM ordersdwh.view_DimCustomers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### DimProduct

-- COMMAND ----------

CREATE TABLE ordersDWH.DimProducts
(
  ProductID INT,
  ProductName String,
  ProductCategory String,
  DimProductsKey INT
)

-- COMMAND ----------

CREATE OR REPLACE VIEW ordersDWH.view_DimProducts
AS
SELECT T.*,row_number() OVER (ORDER BY T.ProductID) as DimProductsKey
FROM
( 
SELECT 
  DISTINCT (ProductID) as ProductID,
 ProductName,
 ProductCategory
FROM
  ordersDWH.trans_sales
) AS T

-- COMMAND ----------

INSERT INTO ordersdwh.DimProducts
SELECT * FROM ordersdwh.view_DimProducts

-- COMMAND ----------

select * from ordersDWH.DimProducts;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### DimRegion

-- COMMAND ----------

CREATE OR REPLACE TABLE ordersDWH.DimRegion (
  RegionID INT,
  RegionName STRING,
  Country String,
  DimRegionKey INT
)

-- COMMAND ----------

CREATE VIEW  view_DimRegion as
  Select T.*, row_number() over (ORDER BY RegionID) as DimRegionKey
  FROM
  (
    Select Distinct(RegionID) as RegionId,
    RegionName,
    Country
  from ordersDWH.trans_sales 
) as T

-- COMMAND ----------

INSERT INTO ordersDWH.DimRegion 
SELECT * from view_DimRegion

-- COMMAND ----------

select * from ordersdwh.dimregion;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DimDate

-- COMMAND ----------

CREATE OR REPLACE TABLE ordersDWH.DimDate
(
  OrderDate DATE,
  DimDateKey INT
)

-- COMMAND ----------

CREATE OR REPLACE VIEW ordersdwh.view_DimDate
AS
SELECT T.*,row_number() OVER (ORDER BY T.OrderDate) as DimDateKey FROM
(
  SELECT
    DISTINCT(OrderDate) as OrderDate
  FROM
  ordersdwh.trans_sales
) AS T

-- COMMAND ----------

INSERT INTO ordersdwh.DimDate
SELECT * FROM ordersDWH.view_DimDate

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Fact Table

-- COMMAND ----------

CREATE TABLE ordersDWH.FactSales
(
  OrderID INT,
  Quantity DECIMAL,
  UnitPrice DECIMAL,
  TotalAmount DECIMAL,
  DimProductsKey INT,
  DimCustomersKey INT,
  DimRegionKey INT,
  DimDateKey INT
  )


-- COMMAND ----------

SELECT
  F.OrderID,
  F.Quantity,
  F.UnitPrice,
  F.TotalAmount,
  DC.DimCustomersKey,
  DP.DimProductsKey,
   DR.DimRegionKey,
  DD.DimDateKey
FROM
  ordersDWH.trans_sales F
LEFT JOIN 
  ordersDWH.DimCustomers DC
  ON F.CustomerID = DC.CustomerID
LEFT JOIN 
  ordersDWH.Dimproducts DP
  ON F.ProductID = DP.ProductID
LEFT JOIN 
  ordersDWH.DimRegion DR
  ON F.Country = DR.Country
LEFT JOIN
  ordersDWH.DimDate DD
  ON F.OrderDate = DD.OrderDate

-- COMMAND ----------


