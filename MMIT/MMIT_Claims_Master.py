# Databricks notebook source
# MAGIC %sh
# MAGIC python3 -m pip install xlrd
# MAGIC pip3 install xlrd
# MAGIC pip3 install --upgrade pyodbc
# MAGIC pip3 install openpyxl
# MAGIC pip install xlrd==1.2.0

# COMMAND ----------

# DBTITLE 1,Importing libraries
import pyodbc
import xlrd
import chardet
import numpy as np
import pandas as pd
from datetime import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from concurrent.futures import TimeoutError
import math

driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager

# COMMAND ----------

# DBTITLE 1,Widget creation
dbutils.widgets.removeAll()
dbutils.widgets.text("jdbcHostname","")
dbutils.widgets.text("jdbcPort","")
dbutils.widgets.text("jdbcDatabase","")
dbutils.widgets.text("keyVaultSecretScope","")
dbutils.widgets.text("sourceName",""),
dbutils.widgets.text("paramTableName","G_US_DATA.DATA_SOURCE_PARAMETER")
dbutils.widgets.text("dataSourceTableName","G_US_DATA.DATA_SOURCE")
dbutils.widgets.text("CurrentTime","")
dbutils.widgets.text("FileName","")

# COMMAND ----------

# DBTITLE 1,Getting values from widgets
# Initialising the variables
jdbcHostname=dbutils.widgets.get("jdbcHostname")
jdbcPort=dbutils.widgets.get("jdbcPort")
jdbcDatabase=dbutils.widgets.get("jdbcDatabase")
keyVaultSecretScope = dbutils.widgets.get("keyVaultSecretScope")
sourceName = dbutils.widgets.get("sourceName")
paramTableName = dbutils.widgets.get("paramTableName")
dataSourceTableName = dbutils.widgets.get("dataSourceTableName")
CurrentTime = dbutils.widgets.get("CurrentTime")
FileName = dbutils.widgets.get("FileName")

# COMMAND ----------

# DBTITLE 1,Establish connection with SQL DB and get source parameters
# Establishing connection to SQL DB
username = dbutils.secrets.get(scope = keyVaultSecretScope , key = "GlobalSQLDB-Login")
pwd = dbutils.secrets.get(scope = keyVaultSecretScope , key = "GlobalSQLDB-Pwd")
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}".format(jdbcHostname, jdbcPort, jdbcDatabase, username,pwd)

# Getting the parameters from DB
df_params= spark.read.jdbc(jdbcUrl,"(select a.* from {0} a INNER JOIN {1} b ON a.DATA_SOURCE_ID = b.DATA_SOURCE_ID WHERE b.DATA_SOURCE_NAME='{2}' or b.DATA_SOURCE_NAME='Global')t".format(paramTableName,dataSourceTableName,sourceName))

display(df_params)

# COMMAND ----------

rawFilePath = df_params.filter(df_params.DATA_SOURCE_PARAMETER_NAME == 'adlsrawmountpath') \
                      .select("DATA_SOURCE_PARAMETER_VALUE") \
                      .first()[0]
foundationSchema = df_params.filter(df_params.DATA_SOURCE_PARAMETER_NAME == 'foundationschema') \
                           .select("DATA_SOURCE_PARAMETER_VALUE") \
                           .first()[0]
foundationPath = df_params.filter(df_params.DATA_SOURCE_PARAMETER_NAME == 'adlsfoundationmountpath') \
                         .select("DATA_SOURCE_PARAMETER_VALUE") \
                         .first()[0]
configTableName = df_params.filter(df_params.DATA_SOURCE_PARAMETER_NAME == 'configtable') \
                          .select("DATA_SOURCE_PARAMETER_VALUE") \
                          .first()[0]
updateExtractedDateSP = df_params.filter(df_params.DATA_SOURCE_PARAMETER_NAME == 'spconfigupdate') \
                                .select("DATA_SOURCE_PARAMETER_VALUE") \
                                .first()[0]
specificationTableName = df_params.filter(df_params.DATA_SOURCE_PARAMETER_NAME == 'specificationtablename') \
                                 .select("DATA_SOURCE_PARAMETER_VALUE") \
                                 .first()[0]
excessfilenamepatternlength = df_params.filter(df_params.DATA_SOURCE_PARAMETER_NAME == 'excessfilenamepatternlength') \
                                      .select("DATA_SOURCE_PARAMETER_VALUE") \
                                      .first()[0]

print(rawFilePath)
print(foundationSchema)
print(foundationPath)
print(configTableName)
print(updateExtractedDateSP)
print(specificationTableName)
print(excessfilenamepatternlength)

# COMMAND ----------

if sourceName == 'Claims' or sourceName == 'IQVIA_LAAD':
   df_config = spark.read.jdbc(jdbcUrl, 
       "(SELECT SI.*, DF.PYTHON_DATE_FORMAT \
       FROM {0} SI \
       INNER JOIN {1} DS ON SI.DATA_SOURCE_ID = DS.DATA_SOURCE_ID \
       LEFT JOIN [G_US_DATA].[H_DATE_FORMAT] DF ON SI.SQL_DATE_COLUMN_FORMAT = DF.SQL_DATE_FORMAT \
       WHERE DS.DATA_SOURCE_NAME = '{2}' \
       AND SI.ACTIVE_FLAG = 1 \
       AND '{3}' LIKE '%' + SI.FILE_NAME_PATTERN + '%') t"
       .format(configTableName, dataSourceTableName, sourceName, FileName))

   display(df_config)

else:
   df_config = spark.read.jdbc(jdbcUrl, 
       "(SELECT SI.*, DF.PYTHON_DATE_FORMAT \
       FROM {0} SI \
       INNER JOIN {1} DS ON SI.DATA_SOURCE_ID = DS.DATA_SOURCE_ID \
       LEFT JOIN [G_US_DATA].[H_DATE_FORMAT] DF ON SI.SQL_DATE_COLUMN_FORMAT = DF.SQL_DATE_FORMAT \
       WHERE DS.DATA_SOURCE_NAME = '{2}' \
       AND SI.ACTIVE_FLAG = 1 \
       AND SUBSTRING('{3}', 1, LEN('{3}') - CAST('{4}' AS INT)) LIKE '%' + SI.FILE_NAME_PATTERN + '%') t"
       .format(configTableName, dataSourceTableName, sourceName, FileName, excessfilenamepatternlength))

   display(df_config)

# added to set the partition flag if we need to write in the lake using partitions.
df_partition_cols = spark.read.jdbc(jdbcUrl, "(select ATTRIBUTE_NAME, PARTITION_COLUMN_ORDERING from {1} SIS WHERE SIS.SOURCE_ITEM_ID = {0} and SIS.PARTITION_COLUMN_ORDERING > 0) t".format(df_config.select('SOURCE_ITEM_ID').first()[0], specificationTableName)).orderBy('PARTITION_COLUMN_ORDERING')
is_Partition = 'Y' if df_partition_cols.count() > 0 else 'N'
print(is_Partition)

# COMMAND ----------

# DBTITLE 1,Creating the Generic functions
# MAGIC %run "Users/tushar.x.saxena@gsk.com/MMIT_latest/GenericFunctions"

# COMMAND ----------

# DBTITLE 1,Create schema
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS FOUNDATION

# COMMAND ----------

# DBTITLE 1,Foundation Logic
try:
   for row in df_config.rdd.toLocalIterator():
       foundationLastUpdated = row.FOUNDATION_LAST_UPDATED
       rawLastUpdated = row.RAW_LAST_UPDATED
       print(foundationLastUpdated)
       print(rawLastUpdated)

       if row.SHEET_NAME is None:
           targetTable = foundationSchema + "." + str.replace(sourceName, '-', '_') + "_" + str.replace(row.SOURCE_ITEM_NAME, ' ', '_')
           foundation_path = foundationPath + row.SOURCE_ITEM_FOUNDATION_PATH + str.replace(row.SOURCE_ITEM_NAME, ' ', '_')
           print(foundation_path)
       else:
           targetTable = foundationSchema + "." + str.replace(sourceName, '-', '_') + "_" + str.replace(str.replace(str.replace(str.replace(row.SOURCE_ITEM_NAME + "_" + row.SHEET_NAME, '-', '_'), ' ', '_'), '(', ''), ')', '')
           foundation_path = foundationPath + row.SOURCE_ITEM_FOUNDATION_PATH + str.replace(row.SOURCE_ITEM_NAME + "_" + row.SHEET_NAME, '-', '_')

       while foundationLastUpdated < rawLastUpdated:
           print('While Loop Foundation Date and Raw Date:', foundationLastUpdated, rawLastUpdated)
           foundationLastUpdated = rawLastUpdated
           filePath = rawFilePath + row.SOURCE_ITEM_RAW_PATH + str.replace(row.SOURCE_ITEM_NAME, ' ', '-') + "/" + str(rawLastUpdated.year) + "/" + rawLastUpdated.strftime("%Y-%b") + "/" + rawLastUpdated.strftime("%d-%b-%Y") + "/"
           print(filePath)
           fileFullPath = ''

           try:
               for file in dbutils.fs.ls(filePath):
                   fileFullPath = filePath + file.name

                   # Minutes folder loop starts here
                   try:
                       for file in dbutils.fs.ls(fileFullPath):
                           fileFullPathMinutes = fileFullPath + file.name

                           # Actual file loop
                           try:
                               for file in dbutils.fs.ls(fileFullPathMinutes):
                                   print("File Path Actual: ", fileFullPathMinutes + file.name)
                                   File_Name_found = "/" + rawLastUpdated.strftime("%d-%b-%Y") + "/" + file.name

                                   if row.FILE_ENCODING_STANDARD is None:
                                       print('BLOCK - Encoding Standard')
                                       rawdata = open('/dbfs' + fileFullPathMinutes + file.name, "rb").read()
                                       result = chardet.detect(rawdata)
                                       charenc = result['encoding']
                                   else:
                                       charenc = row.FILE_ENCODING_STANDARD
                                       print("Character Encoding: ", charenc)

                                   if charenc is None or charenc == "UTF-8-SIG":
                                       charenc = "UTF-8"

                                   if row.FILE_EXTENSION in ('TXT', 'CSV', 'csv', 'txt', ''):
                                       if row.IS_SCHEMA_AVAILABLE == 1:
                                           print('Schema available')
                                           if row.FILE_DECIMAL_SEPERATOR == ',':
                                               print('File decimal separator is comma')
                                               entitySchema = get_schema(row.SOURCE_ITEM_ID)
                                               df = pd.read_csv('/dbfs' + fileFullPathMinutes + File_Name_found, decimal=row.FILE_DECIMAL_SEPERATOR, sep=row.FILE_COLUMN_DELIMITER, encoding=charenc)
                                               new_df = df.dropna(axis=0, how='all')
                                               df_raw_data = spark.createDataFrame(new_df, schema=entitySchema)
                                           else:
                                               entitySchema = get_schema(row.SOURCE_ITEM_ID)
                                               print(row.FILE_EXTENSION)
                                               print('Headers from source Item Specification table')
                                               df_raw_data = spark.read.options(header=True, ignoreTrailingWhiteSpace='true', ignoreLeadingWhiteSpace='true', sep=row.FILE_COLUMN_DELIMITER, encoding=charenc, multiline='true', escape='^', quote='"').csv(fileFullPathMinutes + file.name)
                                               if df_raw_data.count() == 0:
                                                   df_raw_data = spark.createDataFrame(sc.emptyRDD(), schema=entitySchema)
                                       else:
                                           entitySchema = get_schema(row.SOURCE_ITEM_ID)
                                           df_raw_data = spark.createDataFrame(sc.emptyRDD(), schema=entitySchema)

                                   elif row.FILE_EXTENSION == 'parquet':
                                       print("Processing parquet file")
                                       df_raw_data = spark.read.options(header=True, ignoreTrailingWhiteSpace='true', ignoreLeadingWhiteSpace='true', multiline='true').parquet(fileFullPathMinutes + file.name)
                                       if df_raw_data.count() == 0:
                                           df_raw_data = spark.createDataFrame(sc.emptyRDD(), schema=entitySchema)

                                   else:
                                       df_raw_data = spark.read.options(header=True, inferSchema='true', ignoreTrailingWhiteSpace='true', ignoreLeadingWhiteSpace='true', sep=row.FILE_COLUMN_DELIMITER, encoding=charenc, multiline='true').csv(fileFullPathMinutes + file.name).dropDuplicates().dropna(how='all')

                                   df_raw_file_renamed_columns = get_column_renamed(df_raw_data)
                                   sql("SET spark.sql.legacy.timeParserPolicy=LEGACY")
                                   df_raw_file_renamed_columns = Cast_Columns(df_raw_file_renamed_columns, row.SOURCE_ITEM_ID, 'yyyy-MM-dd')
                                   if(sourceName == 'US_RETURN_SAP_COMPLETED_RETURNS_PRA'):
                                        df_raw_file_renamed_columns = df_raw_file_renamed_columns.withColumn("Ord_Create_Date", to_timestamp("Ord_Create_Date","M/d/yyyy")).withColumn("Batch_Expiration_Date", to_timestamp("Batch_Expiration_Date","M/d/yyyy")).withColumn("Requested_deliv_date", to_timestamp("Requested_deliv_date","M/d/yyyy")).withColumn("Billing_Date", to_timestamp("Billing_Date","M/d/yyyy")).withColumn("Document_Date", to_timestamp("Document_Date", "M/d/yyyy"))
                                        df_raw_file_renamed_columns = df_raw_file_renamed_columns.withColumn("Doc_Net_Value",regexp_replace(col("Doc_Net_Value"), ",", "").cast(DecimalType(11, 3))).withColumn("Order_Quantity",regexp_replace(col("Order_Quantity"), ",", "").cast(DecimalType(11, 3))).withColumn("Flu_Eligibility_Qty",regexp_replace(col("Flu_Eligibility_Qty"), ",", "").cast(DecimalType(11, 3))).withColumn("Item_Base_price",regexp_replace(col("Item_Base_price"), ",", "").cast(DecimalType(11, 3))).withColumn("Item_Net_Value",regexp_replace(col("Item_Net_Value"), ",", "").cast(DecimalType(11, 3))).withColumn("Adjusted_Gross",regexp_replace(col("Adjusted_Gross"), ",", "").cast(DecimalType(11, 3))).withColumn("Item_Gross_Value",regexp_replace(col("Item_Gross_Value"), ",", "").cast(DecimalType(11, 3))).withColumn("Subtotal_2",regexp_replace(col("Subtotal_2"), ",", "").cast(DecimalType(11, 3))).withColumn("Subtotal_6",regexp_replace(col("Subtotal_6"), ",", "").cast(DecimalType(11, 3))).withColumn("Doc_Gross_Value",regexp_replace(col("Doc_Gross_Value"), ",", "").cast(DecimalType(11, 3))).withColumn("DocAdjGross",regexp_replace(col("DocAdjGross"), ",", "").cast(DecimalType(11, 3))).withColumn("Orig_Qty",regexp_replace(col("Orig_Qty"), ",", "").cast(DecimalType(11, 3))).withColumn("Cumul_confirmed_qty",regexp_replace(col("Cumul_confirmed_qty"), ",", "").cast(DecimalType(11, 3))).withColumn("Corr_qty",regexp_replace(col("Corr_qty"), ",", "").cast(DecimalType(11, 3))).withColumn("Order_quantity_before_rounding",regexp_replace(col("Order_quantity_before_rounding"), ",", "").cast(DecimalType(11, 3))).withColumn("Target_quantity",regexp_replace(col("Target_quantity"), ",", "").cast(DecimalType(11, 3))).withColumn("Adj_Gross_Delv_Doc",regexp_replace(col("Adj_Gross_Delv_Doc"), ",", "").cast(DecimalType(11, 3))).withColumn("DelvNetVal",regexp_replace(col("DelvNetVal"), ",", "").cast(DecimalType(11, 3))).withColumn("Adj_Gross_Delv_Item",regexp_replace(col("Adj_Gross_Delv_Item"), ",", "").cast(DecimalType(11, 3))).withColumn("DelvItemNet",regexp_replace(col("DelvItemNet"), ",", "").cast(DecimalType(11, 3))) 
                                   df_raw_file_renamed_columns = df_raw_file_renamed_columns.withColumn("Ingestion_Time", lit(foundationLastUpdated))
                                   df_raw_file_renamed_columns = df_raw_file_renamed_columns.withColumn("FILE_NAME", lit(File_Name_found))

                                   CreatingColumnsWithComments(targetTable, df_raw_file_renamed_columns, row.SOURCE_ITEM_ID)
                                   df_raw_file_renamed_columns.write.format("delta").mode("append").option("path", foundation_path).option("mergeSchema", "true").saveAsTable(targetTable)
                                   print("File: {0} loaded to table {1}".format(fileFullPathMinutes + file.name, targetTable))

                               updateFoundationDate(row.SOURCE_ITEM_ID, foundationLastUpdated, jdbcUrl)

                           except Exception as e:
                               if "File/Folder does not exist" in str(e):
                                   print('File path: {0} does not exist or table not found'.format(fileFullPathMinutes))
                               else:
                                   raise e

                   except Exception as e1:
                       if "File/Folder does not exist" in str(e1):
                           print('File path: {0} does not exist or table not found'.format(fileFullPath))
                       else:
                           raise e1

           except Exception as e2:
               if "FileNotFoundException" in str(e2) or "File/Folder does not exist" in str(e2) or "The specified path does not exist" in str(e2):
                   print('File path: {0} does not exist or table not found'.format(filePath))
               else:
                   raise e2

except Exception as e3:
   if "File/Folder does not exist" in str(e3):
       print('4th exception')
   else:
       raise e3

# COMMAND ----------

# DBTITLE 1,Enriched -1 
enrichSchema = df_params.filter(df_params.DATA_SOURCE_PARAMETER_NAME=='enrichedschema').select("DATA_SOURCE_PARAMETER_VALUE").rdd.max()[0]

foundationSchema = df_params.filter(df_params.DATA_SOURCE_PARAMETER_NAME=='foundationschema').select("DATA_SOURCE_PARAMETER_VALUE").rdd.max()[0]

enrichPath = df_params.filter(df_params.DATA_SOURCE_PARAMETER_NAME=='adlsenrichedmounthpath').select("DATA_SOURCE_PARAMETER_VALUE").rdd.max()[0]

configTableName = df_params.filter(df_params.DATA_SOURCE_PARAMETER_NAME=='configtable').select("DATA_SOURCE_PARAMETER_VALUE").rdd.max()[0]

updateExtractedDateSP = df_params.filter(df_params.DATA_SOURCE_PARAMETER_NAME=='spconfigupdate').select("DATA_SOURCE_PARAMETER_VALUE").rdd.max()[0]

specificationTableName = df_params.filter(df_params.DATA_SOURCE_PARAMETER_NAME=='specificationtablename').select("DATA_SOURCE_PARAMETER_VALUE").rdd.max()[0]

excessfilenamepatternlength = df_params.filter(df_params.DATA_SOURCE_PARAMETER_NAME=='excessfilenamepatternlength').select("DATA_SOURCE_PARAMETER_VALUE").rdd.max()[0]

# COMMAND ----------

if sourceName == 'Claims' or sourceName == 'IQVIA_LAAD':
   df_config = spark.read.jdbc(
       jdbcUrl,
       "(SELECT SI.*, DF.PYTHON_DATE_FORMAT \
       FROM {0} SI \
       INNER JOIN {1} DS ON SI.DATA_SOURCE_ID = DS.DATA_SOURCE_ID \
       LEFT JOIN [G_US_DATA].[H_DATE_FORMAT] DF ON SI.SQL_DATE_COLUMN_FORMAT = DF.SQL_DATE_FORMAT \
       WHERE DS.DATA_SOURCE_NAME = '{2}' \
       AND SI.ACTIVE_FLAG = 1 \
       AND '{3}' LIKE '%' + SI.FILE_NAME_PATTERN + '%') t"
       .format(configTableName, dataSourceTableName, sourceName, FileName)
   )
   display(df_config)

else:
   df_config = spark.read.jdbc(
       jdbcUrl,
       "(SELECT SI.*, DF.PYTHON_DATE_FORMAT \
       FROM {0} SI \
       INNER JOIN {1} DS ON SI.DATA_SOURCE_ID = DS.DATA_SOURCE_ID \
       LEFT JOIN [G_US_DATA].[H_DATE_FORMAT] DF ON SI.SQL_DATE_COLUMN_FORMAT = DF.SQL_DATE_FORMAT \
       WHERE DS.DATA_SOURCE_NAME = '{2}' \
       AND SI.ACTIVE_FLAG = 1 \
       AND SUBSTRING('{3}', 1, LEN('{3}') - CAST('{4}' AS INT)) LIKE '%' + SI.FILE_NAME_PATTERN + '%') t"
       .format(configTableName, dataSourceTableName, sourceName, FileName, excessfilenamepatternlength)
   )
   display(df_config)

# COMMAND ----------

# DBTITLE 1,Create Enriched schema
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ENRICHED

# COMMAND ----------

for row in df_config.collect():
   if row.REFRESH_TYPE == 'INCREMENTAL':
       foundationTableName = foundationSchema + "." + sourceName.replace('-', '_') + "_" + row.SOURCE_ITEM_NAME.replace(' ', '_').replace('-', '_')
       enrichedTableName = enrichSchema + "." + sourceName.replace('-', '_') + "_" + row.SOURCE_ITEM_NAME.replace(' ', '_').replace('-', '_')
       
       df_foundationTable = spark.sql(f"DESCRIBE DETAIL {foundationTableName}")
       partitionsList = df_foundationTable.select("partitionColumns").rdd.max()[0]
       
       if len(partitionsList) == 0:
           print(foundationTableName)
           sql_query = f"""
           CREATE TABLE IF NOT EXISTS {enrichedTableName}
           USING DELTA LOCATION '{enrichPath + row.SOURCE_ITEM_ENRICHED_PATH.replace(' ', '_')}' 
           AS SELECT * FROM {foundationTableName} WHERE 1 = 2"""
           spark.sql(sql_query)
           print(sql_query)
       else:
           print('else ')
           partitions = ','.join(partitionsList)
           sql_query = f"""
           CREATE TABLE IF NOT EXISTS {enrichedTableName}
           USING DELTA LOCATION '{enrichPath + row.SOURCE_ITEM_ENRICHED_PATH.replace(' ', '_')}'
           PARTITIONED BY ({partitions})
           AS SELECT * FROM {foundationTableName} WHERE 1 = 2
           """
           spark.sql(sql_query)
           print(sql_query)

# COMMAND ----------

# DBTITLE 1,CID Merge Tables
cdf_cid_rgs_week_xrf_stg_v1 = spark.read.format("delta").load("/mnt/localUS_gen2/Pharma/Local/United States/Commercial/Master Data/usp_cdf_enriched/cdf_cid_rgs_week_xrf_stg_v1/")
cdf_cid_rgs_week_xrf_stg_v1.createOrReplaceTempView("cdf_cid_rgs_week_xrf_stg_v1")

cdf_acct_cid_rgs_wk_xrf_stg_v1 = spark.read.format("delta").load("/mnt/localUS_gen2/Pharma/Local/United States/Commercial/Master Data/usp_cdf_enriched/cdf_acct_cid_rgs_wk_xrf_stg_v1/")
cdf_acct_cid_rgs_wk_xrf_stg_v1.createOrReplaceTempView("cdf_acct_cid_rgs_wk_xrf_stg_v1")

cdf_cid_rgs_rstr_wk_xrf_stg_v = spark.read.format("delta").load("/mnt/localUS_gen2/Pharma/Local/United States/Commercial/Master Data/usp_cdf_enriched/cdf_cid_rgs_rstr_wk_xrf_stg_v/")
cdf_cid_rgs_rstr_wk_xrf_stg_v.createOrReplaceTempView("cdf_cid_rgs_rstr_wk_xrf_stg_v")

cdi_azure_cust_alt_id_data = spark.read.format("delta").load("/mnt/localUS_gen2/Pharma/Local/United States/Commercial/Master Data/usp_cdf_enriched/cdi_azure_cust_alt_id_data/")
cdi_azure_cust_alt_id_data.createOrReplaceTempView("cdi_azure_cust_alt_id_data")

cdf_prod_pln_wk_dim_stg_v = spark.read.format("delta").load("/mnt/localUS_gen2/Pharma/Local/United States/Commercial/Master Data/usp_gpcpm_enriched/cdf_prod_pln_wk_dim_stg_v/")
cdf_prod_pln_wk_dim_stg_v.createOrReplaceTempView("cdf_prod_pln_wk_dim_stg_v")

spark.sql('''Select * from delta.`/mnt/localUS_gen2/Pharma/Local/United States/Commercial/Master Data/usp_cdf_enriched/cdf_cdi_all_cid_mrgs_wk_stg_v1` ''').createOrReplaceTempView('cid_mrg')

sqlContext.sql("create or replace temporary view cid as select a.* from (Select cid,cust_data_src_id as data_src_id from cdf_cid_rgs_week_xrf_stg_v1 where amx_data_src_cd='NPI' union Select cid, acct_data_src_id as data_src_id from cdf_acct_cid_rgs_wk_xrf_stg_v1 where data_src_cd='NPI' ) a");

dx_cid=sqlContext.table('cid')

# COMMAND ----------

# DBTITLE 1,Enriched Logic
try:
   for row in df_config.collect():
       enrichedLastUpdated = row.ENRICHED_LAST_UPDATED
       foundationLastUpdated = row.FOUNDATION_LAST_UPDATED
       rawLastUpdated = row.RAW_LAST_UPDATED

       if enrichedLastUpdated < foundationLastUpdated:
           # Define table names and enriched path based on the presence of SHEET_NAME
           sheet_suffix = "" if row.SHEET_NAME is None else f"_{row.SHEET_NAME.replace(' ', '_').replace('-', '_')}"
           foundationTableName = f"{foundationSchema}.{sourceName.replace('-', '_')}_{row.SOURCE_ITEM_NAME.replace(' ', '_').replace('-', '_')}{sheet_suffix}"
           enrichedTableName = f"{enrichSchema}.{sourceName.replace('-', '_')}_{row.SOURCE_ITEM_NAME.replace(' ', '_').replace('-', '_')}{sheet_suffix}"
           enriched_path = enrichPath + row.SOURCE_ITEM_ENRICHED_PATH + row.SOURCE_ITEM_NAME.replace(' ', '_') + sheet_suffix

           if row.REFRESH_TYPE == 'FULL':
               if(sourceName == 'IQVIA_LAAD' and row.SOURCE_ITEM_NAME == 'PATIENT'):
                   df_data = spark.sql(f"""
                   SELECT * FROM {foundationTableName}
                   WHERE Ingestion_time >= '{rawLastUpdated}'""")
                   df_data = df_data.withColumn("PATIENT_BIRTH_YEAR", when(col("PATIENT_BIRTH_YEAR").isNull(), None).when(col("patient_birth_year") == 0, 0).when((lit(datetime.now().year) - col("patient_birth_year")) <= 85, col("patient_birth_year")).otherwise(concat(lit("<"),lit(datetime.now().year - 85).cast("string"))))
               elif(sourceName == 'IQVIA_LAAD' and row.SOURCE_ITEM_NAME == 'GEOGRAPHY'):
                   df_data = spark.sql(f"""
                   SELECT * FROM {foundationTableName}
                   WHERE Ingestion_time >= '{rawLastUpdated}'""")
                   df_data = df_data.withColumn("CBSA_CODE",lit(None).cast("string"))
                   df_data = df_data.withColumn("CBSA_DESCRIPTION",lit(None).cast("string"))
               elif(sourceName == 'IQVIA_LAAD' and row.SOURCE_ITEM_NAME == 'PATIENT_COMMERCIAL'):
                   df_data = spark.sql(f"""
                   SELECT * FROM {foundationTableName}
                   WHERE Ingestion_time >= '{rawLastUpdated}'""")
                   df_data = df_data.withColumn('DEDUCTIBLE_START_DATE', date_format(df_data['DEDUCTIBLE_START_DATE'], 'yyyyMMdd'))
                   df_data = df_data.withColumn('COPAY_START_DATE', date_format(df_data['COPAY_START_DATE'], 'yyyyMMdd'))
               elif(sourceName == 'IQVIA_LAAD' and row.SOURCE_ITEM_NAME == 'PATIENT_MPD'):
                   df_data = spark.sql(f"""
                   SELECT * FROM {foundationTableName}
                   WHERE Ingestion_time >= '{rawLastUpdated}'""")
                   df_data = df_data.withColumn('DEDUCTIBLE_START_DATE', date_format(df_data['DEDUCTIBLE_START_DATE'], 'yyyyMMdd'))
                   df_data = df_data.withColumn('COPAY_START_DATE', date_format(df_data['COPAY_START_DATE'], 'yyyyMMdd'))
                   df_data = df_data.withColumn('GAP_START_DATE', date_format(df_data['GAP_START_DATE'], 'yyyyMMdd'))
                   df_data = df_data.withColumn('CATASTROPHIC_START_DATE', date_format(df_data['CATASTROPHIC_START_DATE'], 'yyyyMMdd'))
               elif sourceName == 'Lab' and row.SOURCE_ITEM_NAME == 'Patient_Lab':
                   print(f"Loading data for SOURCE_ITEM: {row.SOURCE_ITEM_NAME}")
                   df_data = spark.sql(f"""
                       SELECT a.*, NVL(b.cid, '7777777777') AS HCP_CID
                       FROM {foundationTableName} a
                       LEFT JOIN cdf_cid_rgs_week_xrf_stg_v1 b
                           ON a.PROVIDER_NPI = b.cust_data_src_id
                           AND a.PROVIDER_NPI IS NOT NULL
                           AND b.amx_data_src_cd = 'NPI'
                   """)
               elif sourceName == 'Komodo_LAB' and row.SOURCE_ITEM_NAME == 'LAB_RESULTS_PROGNOS':
                   print(f"Loading data for SOURCE_ITEM: {row.SOURCE_ITEM_NAME}")
                   df_data = spark.sql(f"""
                       SELECT a.*, NVL(b.cid, '7777777777') AS HCP_ID
                       FROM {foundationTableName} a
                       LEFT JOIN cdf_cid_rgs_week_xrf_stg_v1 b
                           ON a.ORDERING_NPI = b.cust_data_src_id
                           AND a.ORDERING_NPI IS NOT NULL
                           AND b.amx_data_src_cd = 'NPI'
                       WHERE Ingestion_time >= '{rawLastUpdated}'
                   """)
               elif sourceName == 'Komodo_LAB' and row.SOURCE_ITEM_NAME == 'LAB_RESULTS_QUEST':
                   print(f"Loading data for SOURCE_ITEM: {row.SOURCE_ITEM_NAME}")
                   df_data = spark.sql(f"""
                       SELECT a.*, NVL(b.cid, '7777777777') AS HCP_ID
                       FROM {foundationTableName} a
                       LEFT JOIN cdf_cid_rgs_week_xrf_stg_v1 b
                           ON a.ORDERING_NPI = b.cust_data_src_id
                           AND a.ORDERING_NPI IS NOT NULL
                           AND b.amx_data_src_cd = 'NPI'
                       WHERE Ingestion_time >= '{rawLastUpdated}'
                   """)    
               else:
                   print(enriched_path)
                   df_data = spark.sql(f"""
                   SELECT * FROM {foundationTableName}
                   WHERE Ingestion_time >= '{rawLastUpdated}'""")
               
               print(f"SELECT * FROM {foundationTableName} WHERE Ingestion_time >= '{rawLastUpdated}'")
               print(f"Loading data for entity: {row.SOURCE_ITEM_NAME} having Ingestion_Time {foundationLastUpdated} into table {enrichedTableName}")
               print("Full Overwrite")

               df_data.write.format("delta").mode("overwrite") \
                   .options(overwriteSchema="true", path=enriched_path) \
                   .saveAsTable(enrichedTableName)

           elif row.REFRESH_TYPE == 'APPEND':
               df_data = spark.sql(f"""
                       SELECT * FROM {foundationTableName}
                       WHERE Ingestion_time >= '{rawLastUpdated}'
               """)

               df_data.write.format("delta").mode("append") \
                   .options(path=enriched_path) \
                   .saveAsTable(enrichedTableName)

           elif(row.REFRESH_TYPE == 'UPSERT' and is_Partition=='N'):
               df_enriched = spark.sql(f"""
                    SELECT * FROM (
                        SELECT *, rank() OVER(PARTITION BY {row.SOURCE_ITEM_PRIMARY_KEY} ORDER BY Ingestion_Time DESC) AS Rank 
                        FROM {foundationTableName} 
                    ) T WHERE T.Rank = 1
                """)
               df_enriched = df_enriched.drop("Rank")
               df_enriched.createOrReplaceTempView("df_enriched")
               if sourceName == 'APLD' and row.SOURCE_ITEM_NAME == 'PROVIDER_WEEKLY':
                   print(f"Loading data for SOURCE_ITEM: {row.SOURCE_ITEM_NAME}")
                   df_data = spark.sql(f"""
                        SELECT a.*, NVL(b.cid, '7777777777') AS HCP_ID
                        FROM df_enriched a
                        LEFT JOIN cdf_cid_rgs_week_xrf_stg_v1 b
                            ON a.PROVIDER_ID = b.cust_data_src_id
                            AND a.PROVIDER_ID IS NOT NULL
                            AND b.amx_data_src_cd in ('IMS')
                   """)

               elif sourceName == 'APLD' and row.SOURCE_ITEM_NAME == 'PROVIDER_TIER_WEEKLY':
                   print(f"Loading data for SOURCE_ITEM: {row.SOURCE_ITEM_NAME}")
                   df_data = spark.sql(f"""
                        SELECT a.*, NVL(b.cid, '7777777777') AS HCP_ID
                        FROM df_enriched a
                        LEFT JOIN cdf_cid_rgs_week_xrf_stg_v1 b
                            ON a.PROVIDER_ID = b.cust_data_src_id
                            AND a.PROVIDER_ID IS NOT NULL
                            AND b.amx_data_src_cd in ('IMS')
                   """)
               
               elif (sourceName =='VAC_SFReporting'):
                   print(f"Loading data for SOURCE_ITEM: {row.SOURCE_ITEM_NAME}")
                   df_data = spark.sql(f"""
                        SELECT a.*, NVL(b.cid, '7777777777') AS HCP_ID
                        FROM df_enriched a
                        LEFT JOIN cdf_cid_rgs_week_xrf_stg_v1 b
                            ON a.PROVIDER_ID = b.cust_data_src_id
                            AND a.PROVIDER_ID IS NOT NULL
                            AND b.amx_data_src_cd in ('IMS')
                   """)
                   
               elif sourceName == 'APLD_SPP' and row.SOURCE_ITEM_NAME == 'PROVIDER_SPP_WEEKLY':
                   print(f"Loading data for SOURCE_ITEM: {row.SOURCE_ITEM_NAME}")
                   df_data = spark.sql(f"""
                        SELECT a.*, NVL(b.cid, '7777777777') AS HCP_ID
                        FROM df_enriched a
                        LEFT JOIN cdf_cid_rgs_week_xrf_stg_v1 b
                            ON a.PROVIDER_ID = b.cust_data_src_id
                            AND a.PROVIDER_ID IS NOT NULL
                            AND b.amx_data_src_cd in ('IMS')
                   """)
               elif sourceName == 'APLD_SPP' and row.SOURCE_ITEM_NAME == 'RxFACT_SPP_WEEKLY':
                   print(f"Loading data for SOURCE_ITEM: {row.SOURCE_ITEM_NAME}")
                   df_cid = spark.sql(f"""
                        SELECT a.*, NVL(b.cid, '7777777777') AS HCP_ID
                        FROM df_enriched a
                        LEFT JOIN cdf_cid_rgs_week_xrf_stg_v1 b
                            ON a.PROVIDER_ID = b.cust_data_src_id
                            AND a.PROVIDER_ID IS NOT NULL
                            AND b.amx_data_src_cd in ('IMS')
                   """)
                   df_cid.createOrReplaceTempView("enrichtmpcid")
                   df_data = spark.sql(f"""
                        SELECT 
                            a.*,
                            NVL(p.prod_id, '7777777777') AS prod_id,
                            NVL(p.brnd_id, '7777777777') AS brnd_id
                        FROM enrichtmpcid a
                        LEFT JOIN cdf_prod_pln_wk_dim_stg_v p
                            ON a.NDC = p.ndc_cd
                            AND a.NDC IS NOT NULL
                   """)
               elif sourceName == 'APLD_SPP' and row.SOURCE_ITEM_NAME == 'DxFACT_SPP_WEEKLY':
                   print(f"Loading data for SOURCE_ITEM: {row.SOURCE_ITEM_NAME}")
                   df_cid = spark.sql(f"""
                        SELECT a.*, NVL(b.cid, '7777777777') AS GSK_PROVIDER_BILLING_ID
                        FROM df_enriched a
                        LEFT JOIN cdf_cid_rgs_week_xrf_stg_v1 b
                            ON a.PROVIDER_BILLING_ID = b.cust_data_src_id
                            AND a.PROVIDER_BILLING_ID IS NOT NULL
                            AND b.amx_data_src_cd in ('IMS')
                    """)
                   df_cid.createOrReplaceTempView("enrichtmpcid")
                   df_cid1 = spark.sql(f"""
                        SELECT a.*, NVL(b.cid, '7777777777') AS GSK_PROVIDER_RENDERING_ID
                        FROM enrichtmpcid a
                        LEFT JOIN cdf_cid_rgs_week_xrf_stg_v1 b
                            ON a.PROVIDER_RENDERING_ID = b.cust_data_src_id
                            AND a.PROVIDER_RENDERING_ID IS NOT NULL
                            AND b.amx_data_src_cd in ('IMS')
                    """)
                   df_cid1.createOrReplaceTempView("enrichtmpcid1")
                   df_cid2 = spark.sql(f"""
                        SELECT a.*, NVL(b.cid, '7777777777') AS GSK_PROVIDER_REFERRING_ID
                        FROM enrichtmpcid1 a
                        LEFT JOIN cdf_cid_rgs_week_xrf_stg_v1 b
                            ON a.PROVIDER_REFERRING_ID = b.cust_data_src_id
                            AND a.PROVIDER_REFERRING_ID IS NOT NULL
                            AND b.amx_data_src_cd in ('IMS')
                    """)
                   df_cid2.createOrReplaceTempView("enrichtmpcid2")
                   df_data = spark.sql(f"""
                        SELECT 
                            a.*,
                            NVL(p.prod_id, '7777777777') AS prod_id,
                            NVL(p.brnd_id, '7777777777') AS brnd_id
                        FROM enrichtmpcid2 a
                        LEFT JOIN cdf_prod_pln_wk_dim_stg_v p
                            ON a.NDC_CD = p.ndc_cd
                            AND a.NDC_CD IS NOT NULL
                    """)
               elif sourceName == 'APLD_SPP' and row.SOURCE_ITEM_NAME == 'PROVIDER_TIER_SPP_WEEKLY':
                   print(f"Loading data for SOURCE_ITEM: {row.SOURCE_ITEM_NAME}")
                   df_data = spark.sql(f"""
                        SELECT a.*, NVL(b.cid, '7777777777') AS HCP_ID
                        FROM df_enriched a
                        LEFT JOIN cdf_cid_rgs_week_xrf_stg_v1 b
                            ON a.PROVIDER_ID = b.cust_data_src_id
                            AND a.PROVIDER_ID IS NOT NULL
                            AND b.amx_data_src_cd in ('IMS')
                   """)
               elif sourceName == 'Claims' and row.SOURCE_ITEM_NAME == 'mx':
                   print(f"Loading data for SOURCE_ITEM: {row.SOURCE_ITEM_NAME}")
                   df_data = spark.sql("""
                       SELECT a.*, 
                              NVL(b.cid, '7777777777') AS GSK_Provider_rendering_id
                       FROM df_enriched a
                       LEFT JOIN cdf_cid_rgs_week_xrf_stg_v1 b
                           ON a.rendering_provider = b.cust_data_src_id
                           AND a.rendering_provider IS NOT NULL
                           AND b.amx_data_src_cd = 'NPI'
                   """)
                   df_data.createOrReplaceTempView("enrichtmpcid")
                   df_data = spark.sql("""
                       SELECT a.*, 
                              NVL(b.cid, '7777777777') AS GSK_Provider_referring_id
                       FROM enrichtmpcid a
                       LEFT JOIN cdf_cid_rgs_week_xrf_stg_v1 b
                           ON a.referring_provider = b.cust_data_src_id
                           AND a.referring_provider IS NOT NULL
                           AND b.amx_data_src_cd = 'NPI'
                   """)
                   df_data.createOrReplaceTempView("enrichtmpcid1")
                   df_data = spark.sql("""
                       SELECT a.*, 
                              NVL(b.cid, '7777777777') AS GSK_Provider_attending_id
                       FROM enrichtmpcid1 a
                       LEFT JOIN cdf_cid_rgs_week_xrf_stg_v1 b
                           ON a.attending_provider = b.cust_data_src_id
                           AND a.attending_provider IS NOT NULL
                           AND b.amx_data_src_cd = 'NPI'
                   """)
                   df_data.createOrReplaceTempView("enrichtmpcid2")
                   df_data = spark.sql("""
                       SELECT a.*, 
                              NVL(b.cid, '7777777777') AS GSK_Provider_operating_id
                       FROM enrichtmpcid2 a
                       LEFT JOIN cdf_cid_rgs_week_xrf_stg_v1 b
                           ON a.operating_provider = b.cust_data_src_id
                           AND a.operating_provider IS NOT NULL
                           AND b.amx_data_src_cd = 'NPI'
                   """)
                   df_data.createOrReplaceTempView("enrichtmpcid3")
                   df_data = spark.sql("""
                       SELECT a.*, 
                              NVL(b.cid, '7777777777') AS GSK_Provider_billing_id
                       FROM enrichtmpcid3 a
                       LEFT JOIN cdf_cid_rgs_week_xrf_stg_v1 b
                           ON a.billing_provider = b.cust_data_src_id
                           AND a.billing_provider IS NOT NULL
                           AND b.amx_data_src_cd = 'NPI'
                   """)
                   df_data.createOrReplaceTempView("enrichtmpcid4")
                   df_data = spark.sql("""
                       SELECT a.*, 
                              NVL(b.cid, '7777777777') AS GSK_Provider_facility_id
                       FROM enrichtmpcid4 a
                       LEFT JOIN cdf_cid_rgs_week_xrf_stg_v1 b
                           ON a.facility_provider = b.cust_data_src_id
                           AND a.facility_provider IS NOT NULL
                           AND b.amx_data_src_cd = 'NPI'
                   """)
                # Handle Claims 'rx'
               elif sourceName == 'Claims' and row.SOURCE_ITEM_NAME == 'rx':
                   print(f"Loading data for SOURCE_ITEM: {row.SOURCE_ITEM_NAME}")
                   df_data = spark.sql(f"""
                       SELECT a.*, NVL(b.cid, '7777777777') AS HCP_ID
                       FROM df_enriched a
                       LEFT JOIN cdf_cid_rgs_week_xrf_stg_v1 b
                           ON a.prescriber_id = b.cust_data_src_id
                           AND a.prescriber_id IS NOT NULL
                           AND b.amx_data_src_cd = 'NPI'
                   """)
               elif(sourceName == 'IQVIA_LAAD' and row.SOURCE_ITEM_NAME == 'Rx_FACT'):
                   print(f"Loading data for SOURCE_ITEM: {row.SOURCE_ITEM_NAME}")
                   df_data = spark.sql(f"""
                       SELECT a.*, NVL(b.cid, '7777777777') AS HCP_CID
                       FROM df_enriched a
                       LEFT JOIN cdf_cid_rgs_week_xrf_stg_v1 b
                           ON a.PROVIDER_ID = b.cust_data_src_id
                           AND a.PROVIDER_ID IS NOT NULL
                           AND b.amx_data_src_cd = 'IMS'
                   """)
                   df_data = df_data.withColumn("INITIAL_CLAIM_ID",lit(None).cast("string"))
                   df_data = df_data.withColumn('FILL_DATE', date_format(df_data['FILL_DATE'], 'yyyyMMdd'))
               elif(sourceName == 'IQVIA_LAAD' and row.SOURCE_ITEM_NAME == 'Mx_Remit_FACT'):
                   print(f"Loading data for SOURCE_ITEM: {row.SOURCE_ITEM_NAME}")
                   df_cid = spark.sql("""
                       SELECT a.*, NVL(b.cid, '7777777777') AS GSK_PROVIDER_RENDERING_ID
                       FROM df_enriched a
                       LEFT JOIN cdf_cid_rgs_week_xrf_stg_v1 b
                           ON a.RENDERING_PROVIDER_ID = b.cust_data_src_id
                           AND a.RENDERING_PROVIDER_ID IS NOT NULL
                           AND b.amx_data_src_cd = 'IMS'
                   """)
                   df_cid.createOrReplaceTempView("enrichtmpcid")
                   df_cid1 = spark.sql("""
                       SELECT a.*, NVL(b.cid, '7777777777') AS GSK_PROVIDER_REFERRING_ID
                       FROM enrichtmpcid a
                       LEFT JOIN cdf_cid_rgs_week_xrf_stg_v1 b
                           ON a.REFERRING_PROVIDER_ID = b.cust_data_src_id
                           AND a.REFERRING_PROVIDER_ID IS NOT NULL
                           AND b.amx_data_src_cd = 'IMS'
                   """)
                   df_cid1.createOrReplaceTempView("enrichtmpcid1")
                   df_cid2 = spark.sql("""
                       SELECT a.*, NVL(b.cid, '7777777777') AS GSK_PROVIDER_BILLING_ID
                       FROM enrichtmpcid1 a
                       LEFT JOIN cdf_cid_rgs_week_xrf_stg_v1 b
                           ON a.BILLING_PROVIDER_ID = b.cust_data_src_id
                           AND a.BILLING_PROVIDER_ID IS NOT NULL
                           AND b.amx_data_src_cd = 'IMS'
                   """)
                   df_cid2.createOrReplaceTempView("enrichtmpcid2")
                   df_data = spark.sql("""
                       SELECT a.*, NVL(b.cid, '7777777777') AS GSK_PROVIDER_FACILITY_ID
                       FROM enrichtmpcid2 a
                       LEFT JOIN cdf_cid_rgs_week_xrf_stg_v1 b
                           ON a.FACILITY_PROVIDER_ID = b.cust_data_src_id
                           AND a.FACILITY_PROVIDER_ID IS NOT NULL
                           AND b.amx_data_src_cd = 'IMS'
                   """)
                   df_data = df_data.withColumn("PLACE_OF_SERVICE_CODE",lit(None).cast("string"))
                   df_data = df_data.withColumn('SERVICE_DATE', date_format(df_data['SERVICE_DATE'], 'yyyyMMdd'))
                   df_data = df_data.withColumn('RECEIVED_DATE', date_format(df_data['RECEIVED_DATE'], 'yyyyMMdd'))
                   df_data = df_data.withColumn('PRI_CHECKED_DATE', date_format(df_data['PRI_CHECKED_DATE'], 'yyyyMMdd'))
                   df_data = df_data.withColumn('SEC_CHECKED_DATE', date_format(df_data['SEC_CHECKED_DATE'], 'yyyyMMdd'))
               elif sourceName == 'IQVIA_GEPO' and row.SOURCE_ITEM_NAME == 'GEPO_HCP_LEVEL_REPORT':
                   print(f"Loading data for SOURCE_ITEM: {row.SOURCE_ITEM_NAME}")
                   df_data = spark.sql(f"""
                       SELECT a.*, NVL(b.cid, '7777777777') AS HCP_ID
                       FROM df_enriched a
                       LEFT JOIN cdf_cid_rgs_week_xrf_stg_v1 b
                           ON a.PRESCRIBER_ID = b.cust_data_src_id
                           AND a.PRESCRIBER_ID IS NOT NULL
                           AND b.amx_data_src_cd = 'IMS'
                   """)
               elif sourceName == 'Komodo_LAB' and row.SOURCE_ITEM_NAME == 'KOMODO_LAB_RESULTS':
                   print(f"Loading data for SOURCE_ITEM: {row.SOURCE_ITEM_NAME}")
                   df_data = spark.sql(f"""
                       SELECT a.*, NVL(b.cid, '7777777777') AS HCP_ID
                       FROM df_enriched a
                       LEFT JOIN cdf_cid_rgs_week_xrf_stg_v1 b
                           ON a.ORDERING_NPI = b.cust_data_src_id
                           AND a.ORDERING_NPI IS NOT NULL
                           AND b.amx_data_src_cd = 'NPI'
                   """)
               elif sourceName == 'Claims_Mx_Provider_Fact' and row.SOURCE_ITEM_NAME == 'MX_PROVIDER_FACT':
                   print(f"Loading data for SOURCE_ITEM: {row.SOURCE_ITEM_NAME}")
                   df_data = spark.sql(f"""
                        SELECT a.*, NVL(b.cid, '7777777777') AS HCP_ID
                        FROM df_enriched a
                        LEFT JOIN cdf_cid_rgs_week_xrf_stg_v1 b
                            ON a.PROVIDER_NPI = b.cust_data_src_id
                            AND a.PROVIDER_NPI IS NOT NULL
                            AND b.amx_data_src_cd in ('NPI')
                   """)
               else:
                   df_data = df_enriched

               df_data.write.format("delta").mode("overwrite") \
                    .options(overwriteSchema="true", path=enriched_path) \
                    .saveAsTable(enrichedTableName)
            
            # created for upsert and partition logic. writes using parallelism
           elif(row.REFRESH_TYPE == 'UPSERT' and is_Partition=='Y'):
               stagingpath=foundation_path+'_STG'
               stagingTableName=foundationTableName+'_STG'

               if delta_exists(stagingpath)==False:
                 df_data_stg = spark.sql(f"""SELECT * FROM (SELECT *, rank() OVER(PARTITION BY {row.SOURCE_ITEM_PRIMARY_KEY} ORDER BY Ingestion_Time DESC) AS Rank FROM {foundationTableName}  ) T WHERE T.Rank = 1""").drop("Rank")
               else:
                 df_data_stg = spark.sql(f"""SELECT * FROM {foundationTableName} WHERE Ingestion_time >= '{rawLastUpdated}' """)

               CreatingColumnsWithCommentsPartitions(stagingTableName, df_data_stg, row.SOURCE_ITEM_ID,stagingpath)
               write_delta_partitions(df_data_stg, stagingpath, stagingTableName,row.SOURCE_ITEM_ID, 'overwrite')
               partition_Keys= [row["ATTRIBUTE_NAME"] for row in df_partition_cols.collect()]
               partitions_cols_str = ",".join(partition_Keys)
               ingestionKey = row.SOURCE_ITEM_PRIMARY_KEY
               
               if sourceName == 'APLD' and row.SOURCE_ITEM_NAME == 'RxFACT_WEEKLY':
                 spark.sql(f"""CREATE TABLE IF NOT EXISTS {enrichedTableName} 
                             USING DELTA 
                             PARTITIONED BY ({partitions_cols_str}) 
                             LOCATION '{enriched_path}'
                             AS SELECT *, 
                             CAST(NULL AS STRING) AS HCP_ID, 
                             CAST(NULL AS STRING) AS prod_id,  
                             CAST(NULL AS STRING) AS brnd_id 
                             FROM {foundationTableName} 
                             WHERE 1=2""")
               
               elif sourceName == 'APLD' and row.SOURCE_ITEM_NAME == 'DxFACT_WEEKLY':
                 spark.sql(f"""CREATE TABLE IF NOT EXISTS {enrichedTableName} 
                             USING DELTA 
                             PARTITIONED BY ({partitions_cols_str}) 
                             LOCATION '{enriched_path}'
                             AS SELECT *, 
                             CAST(NULL AS STRING) AS GSK_PROVIDER_BILLING_ID, 
                             CAST(NULL AS STRING) AS GSK_PROVIDER_RENDERING_ID, 
                             CAST(NULL AS STRING) AS GSK_PROVIDER_REFERRING_ID, 
                             CAST(NULL AS STRING) AS prod_id,  
                             CAST(NULL AS STRING) AS brnd_id 
                             FROM {foundationTableName} 
                             WHERE 1=2""")

               elif sourceName == 'APLD' and row.SOURCE_ITEM_NAME == 'PATIENT_ACTIVE_IND_WEEKLY':
                 spark.sql(f"""CREATE TABLE IF NOT EXISTS {enrichedTableName} 
                             USING DELTA 
                             PARTITIONED BY ({partitions_cols_str}) 
                             LOCATION '{enriched_path}'
                             AS SELECT *
                             FROM {foundationTableName} 
                             WHERE 1=2""")

               elif sourceName == 'Komodo_PLAD' and row.SOURCE_ITEM_NAME == 'MEDICAL_EVENTS':
                   spark.sql(f"""CREATE TABLE IF NOT EXISTS {enrichedTableName} 
                                USING DELTA 
                                PARTITIONED BY ({partitions_cols_str}) 
                                LOCATION '{enriched_path}'
                                AS SELECT *, 
                                CAST(NULL AS STRING) AS GSK_Provider_Rendering_id, 
                                CAST(NULL AS STRING) AS GSK_Provider_Referring_id, 
                                CAST(NULL AS STRING) AS GSK_Provider_Billing_Id, 
                                CAST(NULL AS STRING) AS Product_ID
                                FROM {foundationTableName} 
                                WHERE 1=2""")

               elif sourceName == 'Komodo_PLAD' and row.SOURCE_ITEM_NAME == 'PHARMACY_EVENTS':
                   spark.sql(f"""CREATE TABLE IF NOT EXISTS {enrichedTableName} 
                                USING DELTA 
                                PARTITIONED BY ({partitions_cols_str}) 
                                LOCATION '{enriched_path}'
                                AS SELECT *, 
                                CAST(NULL AS STRING) AS HCP_CID, 
                                CAST(NULL AS STRING) AS Product_ID
                                FROM {foundationTableName} 
                                WHERE 1=2""") 
                   
               distinct_pks = [pk[0] for pk in spark.table(stagingTableName).select(ingestionKey).distinct().orderBy(ingestionKey).collect()]
               total_pks = len(distinct_pks)
               print(f"Found {total_pks} distinct {ingestionKey} values")
               max_parallel = 4
               batch_size = math.ceil(total_pks / 15)
               batches = [distinct_pks[i:i+batch_size] for i in range(0, total_pks, batch_size)]
               print(f"Split into {len(batches)} batches with size {batch_size}")
               batch_ranges = [(batch[0],batch[-1]) for batch in batches]
               print(f"Loading data for SOURCE_ITEM: {row.SOURCE_ITEM_NAME}")
               if delta_exists(enriched_path):
                   spark.sql(f"TRUNCATE TABLE delta.`{enriched_path}` ")
               EntityDWExecList = []
               for i, (mn_pk, mx_pk) in enumerate(batch_ranges, 1):
                   print(f"Batch {i}: {mn_pk} → {mx_pk}")
                   EntityDWExecList.append(
                       NotebookData(
                           '/Shared/US_MMIT/MMIT_Child', 0,
                           {
                               "enrichedTableName": enrichedTableName,
                               "stagingTableName": stagingTableName,
                               "enrichTableDeltaPath": enriched_path,
                               "IngestionKey": ingestionKey,
                               "max_IngestionKey": mx_pk,
                               "min_IngestionKey": mn_pk,
                               "sourceName": sourceName,
                               "sourceItemName": row.SOURCE_ITEM_NAME,
                               "partitionKeys": partitions_cols_str
                           },
                           retry=1
                       )
                   )
               res = parallelNotebooks(EntityDWExecList, max_parallel)
               try:
                 result = [f.result(timeout=0) for f in res]
               except TimeoutError as e:
                 print("A notebook timed out:", e)
               

           elif row.REFRESH_TYPE == 'INCREMENTAL':
               print('Incremental run')
               print(f"Enriched Last Updated: {enrichedLastUpdated}")
               print(f"Foundation Last Updated: {foundationLastUpdated}")
               print(f"Raw Last Updated: {rawLastUpdated}")

               df_enriched = spark.sql(f"""
                    SELECT * FROM (
                        SELECT *, ROW_NUMBER() OVER(PARTITION BY {row.SOURCE_ITEM_PRIMARY_KEY} ORDER BY Ingestion_Time DESC) AS Rank
                        FROM {foundationTableName}
                        WHERE Ingestion_Time > '{enrichedLastUpdated}' AND Ingestion_Time <= '{foundationLastUpdated}'
                    ) T WHERE T.Rank = 1
                """)
               df_enriched = df_enriched.drop("Rank")
               
               print(f"Loading data for SOURCE_ITEM: {row.SOURCE_ITEM_NAME}")
               df_enriched.createOrReplaceTempView("enrichtmp")

               spark.sql("""
                         MERGE INTO {0} TARGET 
                         USING {1} SOURCE
                         ON {2}
                         WHEN MATCHED THEN UPDATE SET * 
                         WHEN NOT MATCHED THEN INSERT * """.format(enrichedTableName,'enrichtmp',get_primary_keys(row.SOURCE_ITEM_PRIMARY_KEY)))
               
               print("Incremental Load is completed...")

           updateEnrichedDate(row.SOURCE_ITEM_ID, foundationLastUpdated, jdbcUrl)

except Exception as e:
   raise e