# Databricks notebook source
# MAGIC %md Notebook designed specifically for Assignment

# COMMAND ----------

dbutils.widgets.text("env","qa2","env")
dbutils.widgets.text("filename","ecomm-sample-dataset.csv","filename")

# COMMAND ----------

env = dbutils.widgets.get("env")
filename = dbutils.widgets.get("filename")
print("env: ", env)
print("filename: ", filename)

# COMMAND ----------

if (env==""):
  dbutils.notebook.exit("ERROR : Environment is not passed correctly")

# COMMAND ----------

if (filename==""):
  dbutils.notebook.exit("ERROR : Filename is not passed correctly")

# COMMAND ----------

filepath = "/mnt/cdl-{}-sandbox-zone/ds_automation_test/test/{}".format(env,filename)
print("filePath:", filepath)

# COMMAND ----------

rawFile = (spark.read               # The DataFrameReader
   .option("header", "true")       # Use first line of all files as header
   .option("sep", ",")            # Use tab delimiter (default is comma-separator)
   .option("inferSchema", "true")  # Automatically infer data types
   .csv(filepath)                   # Creates a DataFrame from CSV after reading in the file
)

# COMMAND ----------

display(rawFile)

# COMMAND ----------

# MAGIC %md Import python packages

# COMMAND ----------

import pyspark.sql
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md Applying the defined rules

# COMMAND ----------

rawFile = rawFile.withColumn("id",trim(col('Uniq Id'))).drop("Uniq Id").withColumn("selling_price",trim(regexp_replace("Selling Price",'[$]','')).cast("float")).drop("Selling Price").withColumn("sitem",trim(col('Item Sold')).cast("integer")).drop("Item Sold").withColumn("product",initcap(regexp_replace("Product Name",'[ ]',' '))).drop("Product Name").withColumn("product_url",trim(regexp_replace("Product Url",'ref=.*&',''))).drop("Product Url").withColumn("cat",trim(col('Category'))).drop("Category")
rawFile.createOrReplaceTempView("rawData")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM rawData

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM rawData WHERE selling_price < 5

# COMMAND ----------

newDf = rawFile.filter(rawFile.selling_price > 5)
newDf.createOrReplaceTempView("filteredData")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM filteredData WHERE selling_price < 5

# COMMAND ----------

# MAGIC %md Preparing the output data

# COMMAND ----------

outputDf = spark.sql("""SELECT id as `Uniq Id`,
                     selling_price as `Selling Price`,
                     sitem as `Item Sold`,
                     product as `Product Name`,
                     product_url as `Product Url`,
                     cat as `Category`
                     FROM filteredData 
                     """)

# COMMAND ----------

outputPath = "/dbfs/mnt/cdl-{}-sandbox-zone/ds_automation_test/test/output.csv".format(env)
print("Output File Path:", outputPath)

# COMMAND ----------

try:
  files = dbutils.fs.ls(outputPath) #handle file not found exception when the notebook runs for first time
  display(files)
except Exception as ex:
  pass

# COMMAND ----------

if files : 
  dbutils.fs.rm(outputPath,True)

# COMMAND ----------

outputDf.write.mode("overwrite").option("header", "true").option("sep", ",").option("inferSchema", "true").format("csv").save(outputPath)

# COMMAND ----------

try:
  files = dbutils.fs.ls(outputPath) #handle file not found exception when the notebook runs for first time
  display(files)
except Exception as ex:
  pass

# COMMAND ----------

# MAGIC %md Top 5 categories output 

# COMMAND ----------

top5Df = spark.sql("""
SELECT id, selling_price, product, product_url, cat, sitem, topN
   FROM
    (SELECT id, selling_price, product, product_url, cat, sitem,
                  ROW_NUMBER() OVER (PARTITION BY cat ORDER BY sitem DESC) as topN
      FROM filteredData) ranked
   WHERE topN <= 5 AND cat IS NOT NULL;""")

# COMMAND ----------

topOutputPath = "/dbfs/mnt/cdl-{}-sandbox-zone/ds_automation_test/test/top-5-sold-by-category.csv".format(env)
print("Output File Path:", topOutputPath)

# COMMAND ----------

try:
  files = dbutils.fs.ls(topOutputPath) #handle file not found exception 
  display(files)
except Exception as ex:
  pass

# COMMAND ----------

if files : 
  dbutils.fs.rm(topOutputPath,True)

# COMMAND ----------

top5Df.repartition(1).write.mode("overwrite").option("header", "true").option("sep", ",").option("inferSchema", "true").format("csv").save(topOutputPath)

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")
