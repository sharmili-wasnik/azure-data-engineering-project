# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text('enrichedTableName','')
dbutils.widgets.text('stagingTableName','')
dbutils.widgets.text('enrichTableDeltaPath','')
dbutils.widgets.text('IngestionKey','')
dbutils.widgets.text('max_IngestionKey','')
dbutils.widgets.text('min_IngestionKey','')
dbutils.widgets.text('sourceName','')
dbutils.widgets.text('sourceItemName','')
dbutils.widgets.text('partitionKeys','')

# COMMAND ----------

enrichedTableName = dbutils.widgets.get("enrichedTableName")
stagingTableName = dbutils.widgets.get("stagingTableName")
IngestionKey = dbutils.widgets.get("IngestionKey")
max_IngestionKey = dbutils.widgets.get("max_IngestionKey")
min_IngestionKey = dbutils.widgets.get("min_IngestionKey")
sourceName = dbutils.widgets.get("sourceName")
sourceItemName = dbutils.widgets.get("sourceItemName")
enriched_path  = dbutils.widgets.get("enrichTableDeltaPath")
partitionKeys = dbutils.widgets.get("partitionKeys")

partition_cols=partitionKeys.split(',')
print(partition_cols)

# COMMAND ----------

cdf_cid_rgs_week_xrf_stg_v1 = spark.read.format("delta").load("/mnt/localUS_gen2/Pharma/Local/United States/Commercial/Master Data/usp_cdf_enriched/cdf_cid_rgs_week_xrf_stg_v1/")
cdf_cid_rgs_week_xrf_stg_v1.createOrReplaceTempView("cdf_cid_rgs_week_xrf_stg_v1")
cdf_prod_pln_wk_dim_stg_v = spark.read.format("delta").load("/mnt/localUS_gen2/Pharma/Local/United States/Commercial/Master Data/usp_gpcpm_enriched/cdf_prod_pln_wk_dim_stg_v/")
cdf_prod_pln_wk_dim_stg_v.createOrReplaceTempView("cdf_prod_pln_wk_dim_stg_v")

# COMMAND ----------

if sourceName == 'APLD':
    # Drop the view if it exists
    spark.sql("DROP VIEW IF EXISTS plan10table")

    # Then create the new view
    spark.sql("""
CREATE OR REPLACE TEMP VIEW plan10table AS
SELECT
    plan_id AS apldplanid,
    CONCAT(ims_payer_id, RIGHT(CONCAT('0000', LEFT(ims_pln_id, 4)), 4)) AS PLAN10
FROM delta.`/mnt/usenriched/Pharma/Local/United States/Commercial/Claims/IQVIA/APLD/usp_apld_enriched/PLAN_WEEKLY`
""")

# COMMAND ----------

# Increase shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "64MB")

# COMMAND ----------

df_enirch_part = spark.sql(f'''SELECT * FROM {stagingTableName} WHERE {IngestionKey} >='{min_IngestionKey}' and {IngestionKey} <= '{max_IngestionKey}' ''')
df_enirch_part.createOrReplaceTempView("enrichedtemp")

if sourceName == 'APLD' and sourceItemName == 'DxFACT_WEEKLY':  
  df_final_part = spark.sql("""
                            SELECT a.*, coalesce(b.cid, '7777777777') AS GSK_PROVIDER_BILLING_ID,coalesce(c.cid, '7777777777') AS GSK_PROVIDER_RENDERING_ID,coalesce(d.cid, '7777777777') AS GSK_PROVIDER_REFERRING_ID,coalesce(p.prod_id, '7777777777') AS prod_id, coalesce(p.brnd_id,'7777777777') AS brnd_id, cast(PLAN10 as bigint) plan_10 FROM enrichedtemp a 
                            LEFT JOIN (select cust_data_src_id,cid from cdf_cid_rgs_week_xrf_stg_v1  where amx_data_src_cd in ('IMS') and cust_data_src_id is not null ) b
                            ON coalesce(a.PROVIDER_BILLING_ID,'XX') = b.cust_data_src_id
                            LEFT JOIN (select cust_data_src_id,cid from cdf_cid_rgs_week_xrf_stg_v1  where amx_data_src_cd in ('IMS') and cust_data_src_id is not null ) c
                            ON coalesce(a.PROVIDER_RENDERING_ID,'XX') = c.cust_data_src_id
                            LEFT JOIN (select cust_data_src_id,cid from cdf_cid_rgs_week_xrf_stg_v1  where amx_data_src_cd in ('IMS') and cust_data_src_id is not null) d
                            ON coalesce(a.PROVIDER_REFERRING_ID,'XX') = d.cust_data_src_id
                            LEFT JOIN (select ndc_cd,prod_id,brnd_id from cdf_prod_pln_wk_dim_stg_v where ndc_cd is not null ) p
                            ON coalesce(a.NDC_CD,'XX') = p.ndc_cd
                            LEFT JOIN plan10table pl
                            ON a.plan_id = pl.apldplanid
                            """)

elif sourceName == 'APLD' and sourceItemName == 'RxFACT_WEEKLY':
  df_final_part = spark.sql("""
                            SELECT a.*, coalesce(b.cid, '7777777777') AS HCP_ID,coalesce(p.prod_id, '7777777777') AS prod_id, coalesce(p.brnd_id,'7777777777') AS brnd_id, cast(PLAN10 as bigint) plan_10 FROM enrichedtemp a 
                            LEFT JOIN (select cust_data_src_id,cid from cdf_cid_rgs_week_xrf_stg_v1  where amx_data_src_cd in ('IMS') and cust_data_src_id is not null ) b
                            ON coalesce(a.PROVIDER_ID,'XX') = b.cust_data_src_id
                            LEFT JOIN (select ndc_cd,prod_id,brnd_id from cdf_prod_pln_wk_dim_stg_v where ndc_cd is not null ) p
                            ON coalesce(a.NDC,'XX') = p.ndc_cd
                            LEFT JOIN plan10table pl
                            ON a.plan_id = pl.apldplanid
                            """)                

elif sourceName == 'APLD' and sourceItemName == 'PATIENT_ACTIVE_IND_WEEKLY':
  df_final_part = spark.sql("""SELECT * from enrichedtemp """)

elif sourceName == 'Komodo_PLAD' and sourceItemName == 'MEDICAL_EVENTS':
  df_final_part = spark.sql("""
                            SELECT a.*, coalesce(b.cid, '7777777777') AS GSK_Provider_Rendering_id,coalesce(c.cid, '7777777777') AS GSK_Provider_Referring_id,coalesce(d.cid, '7777777777') AS GSK_Provider_Billing_Id,coalesce(p.prod_id, '7777777777') AS Product_ID
                            FROM enrichedtemp a 
                            LEFT JOIN (select cust_data_src_id,cid from cdf_cid_rgs_week_xrf_stg_v1  where amx_data_src_cd in ('NPI') and cust_data_src_id is not null ) b
                            ON coalesce(a.RENDERING_NPI,'XX') = b.cust_data_src_id
                            LEFT JOIN (select cust_data_src_id,cid from cdf_cid_rgs_week_xrf_stg_v1  where amx_data_src_cd in ('NPI') and cust_data_src_id is not null ) c
                            ON coalesce(a.REFERRING_NPI,'XX') = c.cust_data_src_id
                            LEFT JOIN (select cust_data_src_id,cid from cdf_cid_rgs_week_xrf_stg_v1  where amx_data_src_cd in ('NPI') and cust_data_src_id is not null) d
                            ON coalesce(a.BILLING_NPI,'XX') = d.cust_data_src_id
                            LEFT JOIN (select ndc_cd,prod_id from cdf_prod_pln_wk_dim_stg_v where ndc_cd is not null ) p
                            ON coalesce(a.NDC11,'XX') = p.ndc_cd
                            """)
  
elif sourceName == 'Komodo_PLAD' and sourceItemName == 'PHARMACY_EVENTS':
  df_final_part = spark.sql("""
                            SELECT a.*, coalesce(b.cid, '7777777777') AS HCP_CID, coalesce(p.prod_id, '7777777777') AS Product_ID
                            FROM enrichedtemp a 
                            LEFT JOIN (select cust_data_src_id,cid from cdf_cid_rgs_week_xrf_stg_v1  where amx_data_src_cd in ('NPI') and cust_data_src_id is not null ) b
                            ON coalesce(a.PRESCRIBER_NPI,'XX') = b.cust_data_src_id
                            LEFT JOIN (select ndc_cd,prod_id from cdf_prod_pln_wk_dim_stg_v where ndc_cd is not null ) p
                            ON coalesce(a.NDC11,'XX') = p.ndc_cd
                            """)
  

print(f"Loading data for SOURCE_ITEM: {sourceItemName}")
df_final_part.write.format("delta").mode("append").partitionBy(partition_cols).options(path=enriched_path).saveAsTable(enrichedTableName)