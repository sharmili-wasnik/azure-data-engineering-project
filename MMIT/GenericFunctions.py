# Databricks notebook source
def updateDate(key,lastUpdateDate,jdbcUrl,updateExtractedDateSP,updatefield):  
  driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager
  con = driver_manager.getConnection(jdbcUrl)
  query = "EXEC {0} '{1}','{2}','{3}'".format(updateExtractedDateSP,key,updatefield,lastUpdateDate.strftime('%Y-%m-%d %H:%M:%S'))
  print("Executing SQL Query: {0}".format(query))
  exec_statement = con.prepareCall(query)
  exec_statement.execute() 
  # Close connections
  exec_statement.close()
  con.close()

# COMMAND ----------

# DBTITLE 1,Function to update the foundation date in config table
def updateFoundationDate(id,RAW_LAST_UPDATED,jdbcUrl):
  from datetime import datetime
  driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager
  con = driver_manager.getConnection(jdbcUrl)
  query = "EXEC {0} {1},'{2}','{3}'".format(updateExtractedDateSP,id,'FOUNDATION_LAST_UPDATED',RAW_LAST_UPDATED.strftime('%Y-%m-%d %H:%M:%S'))
  print("Executing SQL Query: {0}".format(query))
  exec_statement = con.prepareCall(query)
  exec_statement.execute()
  exec_statement.close()
  con.close()

# COMMAND ----------

# DBTITLE 1,Function to update the enriched date in config table
def updateEnrichedDate(id,enrichedLastUpdated,jdbcUrl):
  driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager
  con = driver_manager.getConnection(jdbcUrl)
  query = "EXEC {0} {1},'{2}','{3}'".format(updateExtractedDateSP,id,'ENRICHED_LAST_UPDATED',enrichedLastUpdated.strftime('%Y-%m-%d %H:%M:%S'))
  print("Executing SQL Query: {0}".format(query))
  exec_statement = con.prepareCall(query)
  exec_statement.execute()
  exec_statement.close()
  con.close()

# COMMAND ----------

# DBTITLE 1,DF to extract year value from date column
#UDF to extract year value from date column
@udf()
def get_year_from_date(dateValue,dateFormat):
  dateValue=str(dateValue)
  dateFormat=str(dateFormat)
  if(dateFormat=='%Y-Qn'):
    return dateValue.split("-")[-2]
  return datetime.strptime(dateValue,dateFormat).year

# COMMAND ----------

# DBTITLE 1,Function to rename the column names
#Function to rename the column names
def get_column_renamed(df):
  for col in df.columns:
    df=df.withColumnRenamed(col,col.replace(" ","_").replace("-","_").replace("_/_","_").replace("/","").replace("o;?","").replace("+","").replace(";","").replace("?","").replace(",","").replace("{","").replace("}","").replace("(","").replace(")","").replace("\n","").replace("\t","").replace("=","").replace(".",""))
  return df

# COMMAND ----------

# DBTITLE 1,Function to frame merge query based on the Primary Keys of the table
#Function to frame merge query based on the Primary Keys of the table
def get_primary_keys(entityPrimaryKey):
  entityPrimaryKey=str(entityPrimaryKey)
  converted_list=entityPrimaryKey.rsplit(",")
  query=""
  for element in converted_list:
    query+="SOURCE."+element+"=TARGET."+element+" AND "
  query= query[:-5]
  return query 

# COMMAND ----------

# DBTITLE 1,Function to frame schema for a source item
def get_schema(sourceItemID):
  df_spec= spark.read.jdbc(jdbcUrl,"(select ATTRIBUTE_NAME,DATA_TYPE from {1} SIS WHERE SIS.SOURCE_ITEM_ID ={0} ORDER BY DATA_ELEMENT_POSITION OFFSET 0 ROWS)t".format(sourceItemID,specificationTableName))
  entitySchema=StructType([
  StructField(ATTRIBUTE_NAME, eval(DATA_TYPE), True) for (ATTRIBUTE_NAME, DATA_TYPE) in  df_spec.rdd.collect()
  ])
  return entitySchema

# COMMAND ----------

def dbsfileread(filePath,SOURCE_ITEM_NAME,name):
  #dfColumnConfig=spark.read.jdbc(jdbcUrl,"(Select * from [G_ALL_KCTRANSF].[ENTITY_METADATA] where Entity_Name='{0}')t".format(Entity_Name))

  rawfilePath = '/dbfs'+filePath+name
  spark = SparkSession.builder.getOrCreate()
  #path='/dbfs/FileStore/tables/APP093A.DBF'
  dbf = Dbf5(rawfilePath)
  df = dbf.to_dataframe()
  spark_df = spark.createDataFrame(df)
  #spark_df.createOrReplaceTempView('sampledata')
  return(spark_df)

# COMMAND ----------

def Cast_Columns(df_raw_data,sourceItemID,dateFormat):
  df_spec= spark.read.jdbc(jdbcUrl,"(select * from {1} SIS WHERE SIS.SOURCE_ITEM_ID ={0} ORDER BY CAST(DATA_ELEMENT_POSITION AS INT) OFFSET 0 ROWS)t".format(sourceItemID,specificationTableName))
  datafile_schema=df_raw_data.schema  
  for index,item in enumerate(datafile_schema.fields):
    if isinstance(item.dataType, ArrayType):
      continue
    for row in df_spec.rdd.collect():
      if(int(row.DATA_ELEMENT_POSITION) == index+1):
        df_raw_data_casted = df_raw_data
        if(str(row.DATA_TYPE) != str(item.dataType)+'()'):
          if(str(row.DATA_TYPE)== 'TimestampType()'):
            df_raw_data_casted = df_raw_data.withColumn(item.name, unix_timestamp(col(item.name).cast("string"),dateFormat).cast("timestamp"))
          elif(str(row.DATA_TYPE) == 'DateType()'):
            df_raw_data_casted = df_raw_data.withColumn(item.name, to_date(unix_timestamp(col(item.name).cast("string"),dateFormat).cast("timestamp")))
          elif(str(row.DATA_TYPE).startswith('Decimal')):
            df_raw_data_casted = df_raw_data.withColumn(item.name, col(item.name).cast(str.replace(row.DATA_TYPE,'Type','')))
          else:
            df_raw_data_casted = df_raw_data.withColumn(item.name, col(item.name).cast(str.replace(row.DATA_TYPE,'Type()','')))
          df_raw_data = df_raw_data_casted
        df_raw_data=df_raw_data.withColumnRenamed(item.name,row.ATTRIBUTE_NAME)
  return df_raw_data

# COMMAND ----------

def Cast_Columns_nice(df_raw_data,sourceItemID,dateFormat):
  df_spec= spark.read.jdbc(jdbcUrl,"(select * from {1} SIS WHERE SIS.SOURCE_ITEM_ID ={0} ORDER BY CAST(DATA_ELEMENT_POSITION AS INT) OFFSET 0 ROWS)t".format(sourceItemID,specificationTableName))
  datafile_schema=df_raw_data.schema
  for index,item in enumerate(datafile_schema.fields):
    for row in df_spec.rdd.collect():
      if(int(row.DATA_ELEMENT_POSITION) == index+1):
        df_raw_data_casted = df_raw_data
        if(str(row.DATA_TYPE) != str(item.dataType)+'()'):
          if(str(row.DATA_TYPE)== 'TimestampType()'):
            df_raw_data_casted = df_raw_data.withColumn(item.name, unix_timestamp(col(item.name).cast("string"),dateFormat).cast("timestamp"))
          elif(str(row.DATA_TYPE) == 'DateType()'):
            dateFormat1 = dateFormat.replace(' HH:mm:ss','').replace(' HH:mm','')
            df_raw_data_casted = df_raw_data.withColumn(item.name, to_date(unix_timestamp(col(item.name).cast("string"),dateFormat1).cast("timestamp")))
          elif(str(row.DATA_TYPE).startswith('Decimal')):
            df_raw_data_casted = df_raw_data.withColumn(item.name, col(item.name).cast(str.replace(row.DATA_TYPE,'Type','')))
          else:
            df_raw_data_casted = df_raw_data.withColumn(item.name, col(item.name).cast(str.replace(row.DATA_TYPE,'Type()','')))
          df_raw_data = df_raw_data_casted
        df_raw_data=df_raw_data.withColumnRenamed(item.name,row.ATTRIBUTE_NAME)
  return df_raw_data

# COMMAND ----------

def Cast_DecimalExponential(df_raw_data,sourceItemID):
  df_spec= spark.read.jdbc(jdbcUrl,"(select * from {1} SIS WHERE SIS.SOURCE_ITEM_ID ={0} ORDER BY CAST(DATA_ELEMENT_POSITION AS INT) OFFSET 0 ROWS)t".format(sourceItemID,specificationTableName))
  datafile_schema=df_raw_data.schema  
  for index,item in enumerate(datafile_schema.fields):
    for row in df_spec.rdd.collect():
      if(int(row.DATA_ELEMENT_POSITION) == index+1):
        df_raw_data_casted = df_raw_data
        if(str(row.DATA_TYPE) != str(item.dataType)+'()'):
          if(str(row.DATA_TYPE).startswith('Decimal')):
            df_raw_data_casted = df_raw_data.withColumn(item.name, when(col(item.name) == '0E-12','0.00').when(col(item.name) == '1E-12','1.00').otherwise(col(item.name)))
          else:
            df_raw_data_casted = df_raw_data.withColumn(item.name, col(item.name).cast(str.replace(row.DATA_TYPE,'Type()','')))
          df_raw_data = df_raw_data_casted
        df_raw_data=df_raw_data.withColumnRenamed(item.name,row.ATTRIBUTE_NAME)
  return df_raw_data

# COMMAND ----------

def convert_function(df_raw_data,SOURCE_ITEM_ID):
  #print('one')
  df_raw_file_renamed_columns = get_column_renamed(df_raw_data)
  sql("SET spark.sql.legacy.timeParserPolicy=LEGACY") 
  Final_data=  spark.createDataFrame(sc.emptyRDD(),schema=get_schema(SOURCE_ITEM_ID))
  Final_data=get_column_renamed(Final_data)
  tgtdatatyp =  Final_data.dtypes

  
  for i , dtyp in tgtdatatyp:
    j ='df_raw_file_renamed_columns["' + i + '"]'
    if(dtyp=="date"):
      #print('date is processed')
      df_raw_file_renamed_columns = df_raw_file_renamed_columns.withColumn(i, to_date(col(i)))
      df_raw_file_renamed_columns = df_raw_file_renamed_columns.withColumn(i,from_unixtime(unix_timestamp(i,'yyyyMMddHHmmss')).cast(dtyp))
    if(dtyp=="timestamp"):
      #print('timestamp is processed')
      df_raw_file_renamed_columns = df_raw_file_renamed_columns.withColumn(i,eval(j)[0:17])
      df_raw_file_renamed_columns = df_raw_file_renamed_columns.withColumn(i,to_timestamp(eval(j),"yyyyMMddHHmmssSSS"))   
      #display(df_raw_file_renamed_columns)
    if (dtyp.find('decimal') != -1):   
      #print(dtyp, ' is decimal')
      df_raw_file_renamed_columns = df_raw_file_renamed_columns.withColumn(i,round(i,7)) 
     
    if ((dtyp != "date") & (dtyp != "timestamp") & (dtyp !="decimal")):
      df_raw_file_renamed_columns = df_raw_file_renamed_columns.withColumn(i, eval(j).cast(dtyp))

  
  return df_raw_file_renamed_columns
  #
#display(df_raw_file_renamed_columns)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.functions import col
from pyspark.sql.types import *
from datetime import *

# COMMAND ----------

def processbadrecordfolder(badrecordspath1):
  
  filePath = badrecordspath1
  print('file path is ',filePath)
  for file in dbutils.fs.ls(filePath): 
    fileinner = file.name
    #print(fileinner)
    for file1 in dbutils.fs.ls(filePath+"/"+fileinner): 
      file2 = filePath+"/"+fileinner+file1.name #+ 'bad_records/'
      print(file2)
      for file3 in dbutils.fs.ls(file2):
        print("file 3 is",file3.name)
        file4=file2+file3.name
        file5=file4.split('/')
        print(file5)
        fileactual=file4.split('/')[-4]
        print(file3.name) 
        print("fileactual is ",fileactual)  
        enrich = spark.read.format("json").load(file2+"/"+file3.name)
        dftxt= enrich.select(col("record"))
        dftxt.coalesce(1).write.format("text").option("header", "false").mode("overwrite").save(file2) # WRITE TO TXT FILE
      
  for filetxt in dbutils.fs.ls(file2):
    if(filetxt.name.find(".txt")!= -1):
      print("txt file is ", filetxt.name)
      print(" path is ",file2)
      dbutils.fs.cp(file2+filetxt.name, badrecordspath1+"/"+fileactual+"_"+str(now1)+".txt")
      dbutils.fs.rm(file2+filetxt.name,True) 
    else:
      print("else")
      dbutils.fs.rm(file2+filetxt.name,True)     
        
     
       
      

# COMMAND ----------

@udf
def get_year_from_datestr(dateValue,dateFormat):
  dateValue=str(dateValue)
  print('dateValue '+dateValue)
  dateFormat=str(dateFormat)
  print('dateFormat '+dateFormat)  
  return dateValue.split("/")[-1] 

# COMMAND ----------

def convert_functiondecimal(df_raw_data,SOURCE_ITEM_ID):
  #print('one')
  df_raw_file_renamed_columns = get_column_renamed(df_raw_data) 
  sql("SET spark.sql.legacy.timeParserPolicy=LEGACY") 
  Final_data=  spark.createDataFrame(sc.emptyRDD(),schema=get_schema(SOURCE_ITEM_ID))
  tgtdatatyp =  Final_data.dtypes
  
  for i , dtyp in tgtdatatyp:
    j ='df_raw_file_renamed_columns["' + i + '"]'
    if(dtyp=="date"):    
      #print('date is processed')
      df_raw_file_renamed_columns = df_raw_file_renamed_columns.withColumn(i,from_unixtime(unix_timestamp(i,'yyyyMMddHHmmss')).cast(dtyp))
    if(dtyp=="timestamp"):
      #print('timestamp is processed')
      df_raw_file_renamed_columns = df_raw_file_renamed_columns.withColumn(i,eval(j)[0:17])
      df_raw_file_renamed_columns = df_raw_file_renamed_columns.withColumn(i,to_timestamp(eval(j),"yyyyMMddHHmmssSSS"))   
      #display(df_raw_file_renamed_columns)
    if (dtyp.find("decimal") != -1):
      print(dtyp, " is decimal(22,0) type")
      #df_raw_file_renamed_columns = df_raw_file_renamed_columns.withColumn(i, substring_index(j, '.', 6)) 
    if (dtyp.find("%decimal%") == -1):
      print(dtyp, " == -1 is decimal type")
    if (dtyp.find("%decimal%") == 1):
      print(dtyp, " == 1 is decimal type")
      
    if ((dtyp != "date") & (dtyp != "timestamp")):
      df_raw_file_renamed_columns = df_raw_file_renamed_columns.withColumn(i, eval(j).cast(dtyp))
   
    
      
  
  return df_raw_file_renamed_columns
  # df_raw_data = df_raw_data.withColumn("FILE_NAME", substring_index(input_file_name(), '/', -2)) 
#display(df_raw_file_renamed_columns)

# COMMAND ----------

import numpy as np;
def ReadFixedWidthTEXT(Item_Name,FilePath,Encoding):

  dfColumnConfig=spark.read.jdbc(jdbcUrl,"(Select * from [G_US_DATA].[SOURCE_ITEM_SPECIFICATION] where Item_Name='{0}')t".format(Item_Name))

  filePath = FilePath
#   rawFilePath+str.replace(Entity_Name,' ',' ')+"/"+str(todayDate.year)+"/"+todayDate.strftime("%Y-%b")+"/"+todayDate.strftime("%d-%b-%Y")+"/"
  columns=''
  width='1,'
  StartPosition=''
  EndPosition=''
  print('test',dfColumnConfig.select('PARTIAL_DATA_EXTRACTION').collect()[0][0])
  #display(dfColumnConfig)
  if(dfColumnConfig.select('PARTIAL_DATA_EXTRACTION').collect()[0][0]==True):
    width='0,'
    for row in dfColumnConfig.orderBy(dfColumnConfig["DATA_ELEMENT_POSITION"]).collect():

      columns=columns+row.ATTRIBUTE_NAME+','
      StartPosition=StartPosition+str(row.START_POSITION+1)+','
      EndPosition=EndPosition+str(row.END_POSITION)+','
      width=width+str((int(row.END_POSITION)-int(row.START_POSITION))+1)+','
      #print("Width",width)

  else:    
    for row in dfColumnConfig.orderBy(dfColumnConfig["DATA_ELEMENT_POSITION"]).collect():
      columns=columns+row.ATTRIBUTE_NAME+','
      width=width+row.COLUMN_LENGTH+','
    #print(width)
  columns= columns.rstrip(',').split(",")
  width=width.rstrip(',').split(",")

  schema_dict={
    "definition":{
      "Columns":columns,
      "Width":width
    }}   

  df_dict = dict()
  for file in schema_dict.keys():
      df = spark.read.options(mode=Encoding).text(filePath)
      if(dfColumnConfig.select('PARTIAL_DATA_EXTRACTION').collect()[0][0]==True):
        start_list=StartPosition.rstrip(',').split(",")
      else :
        start_list = np.cumsum(np.asarray(schema_dict[file]["Width"],int)).tolist()[:-1]

#       print(start_list)
  #     print(schema_dict)

      df_dict[file] = df.select(
          [
             df.value.substr(
                    int(start_list[i]), 
                    int(schema_dict[file]["Width"][i+1])
                ).alias(schema_dict[file]["Columns"][i]) for i in range(len(start_list))
          ]
      )
  return df_dict[file]
     

# COMMAND ----------

from pyspark.sql.functions import col, when
def apply_null_values_for_blank_cells(df):
  for val in df.columns:
    df=df.withColumn(val, when(col(val) == '', None).otherwise(col(val)))
  return df

# COMMAND ----------

def PrimaryFieldCountCheck(df_raw,primary_key):
  lstprimarykeys=primary_key.split(',')
  df = df_raw.select(lstprimarykeys) 
  df = df.groupBy(lstprimarykeys).count()
  df = df.filter("count > 1")
  df.show()
  return df

# COMMAND ----------

def MandatoryFieldNullCheck(df_raw,primary_key):
  lstprimarykeys=primary_key.split(',')
  df = df_raw.select(lstprimarykeys)
  str_df = "df.withColumn('NullCheck',"
  str_df += ".".join(["when(df."+i+".isNull(),-1)" for i in lstprimarykeys])
  str_df += ".otherwise(0))"
  df = eval(str_df)
  df = df.filter("NullCheck == -1")
  df.show()
  return df

# COMMAND ----------

def castfunctionCustoms_Report(dfRawData,sourceItemID):
  df_spec= spark.read.jdbc(jdbcUrl,"(select * from {1} SIS WHERE SIS.SOURCE_ITEM_ID ={0} ORDER BY CAST(DATA_ELEMENT_POSITION AS INT) OFFSET 0 ROWS)t".format(entityspecId,specificationTableName))
  df_spec=df_spec.withColumn("DATA_ELEMENT_POSITION",col("DATA_ELEMENT_POSITION").cast("int"))
  df_spec=df_spec.select(col("*")).filter(df_spec.SOURCE_ITEM_ID == entityspecId).orderBy("DATA_ELEMENT_POSITION")
  FileHeader=df_spec.select('ATTRIBUTE_NAME').rdd.flatMap(lambda x: x).collect()
  for i in range(len(FileHeader)):
    FileHeader[i]=FileHeader[i].lower()
  print(FileHeader)
  for clmn in dfRawData.columns:
    dfRawData=dfRawData.withColumnRenamed(clmn,clmn.replace(" #","").replace("/"," ").replace("   "," / "))
  ActualHeader=dfRawData.columns
  for j in range(len(ActualHeader)):
    ActualHeader[j]=ActualHeader[j].lower()
  print(ActualHeader)
  if str(FileHeader).lower() == str(ActualHeader).lower():
    print('Matched')
    spark.sql("INSERT INTO {6} values ('{0}','{1}','{2}','{3}','{4}',{5})".format(row.SOURCE_ITEM_NAME,rawPath,dataQualityRule,'PASSED','HeaderName Matched','current_timestamp()',dqtablename))
  else:
    print("NotMatched")
    list=diff(FileHeader,ActualHeader)
    print(list)
    joinstring=",".join(list)
    print(joinstring)
    if list:
      RuleDesc="HeaderNotMatched"+","+str(joinstring)+" "+"is Different attribute between actual header and File header List."
      print(RuleDesc)
    else:
      RuleDesc="HeaderNotMatched"+"," + "order "+"is Different attribute between actual header and File header List."
      print(RuleDesc)
       
    spark.sql("INSERT INTO {6} values ('{0}','{1}','{2}','{3}','{4}',{5})".format(row.SOURCE_ITEM_NAME,rawPath,dataQualityRule,'FAILED',RuleDesc,'current_timestamp()',dqtablename))
    
  return dfRawData

# COMMAND ----------

def columncount(dfRawData):
  A=len(dfRawData.columns)
  B=fileColumnCount
  if A == B:
    print("Column Count Matched")
    spark.sql("INSERT INTO {6} values ('{0}','{1}','{2}','{3}','{4}',{5})".format(row.SOURCE_ITEM_NAME,rawPath,dataQualityRule,'PASSED','ColumnCountMatched','current_timestamp',dqtablename))
  else:
    print("Count Not Matched")
    Comment="CountNotMatched"+","+"Actual File Count is"+" "+str(B)+" "+"but received File count is "+str(A)
    spark.sql("INSERT INTO {6} values ('{0}','{1}','{2}','{3}','{4}',{5})".format(row.SOURCE_ITEM_NAME,rawPath,dataQualityRule,'FAILED',Comment,'current_timestamp',dqtablename))
  return dfRawData

# COMMAND ----------

def CopyfiletoBlob(mntpath,tmppath,ENTITY_NAME,CurrentDate,fileformat):
  i = 1
  #fileformat = "." + fileformat
  for file in dbutils.fs.ls(tmppath):
    filename = (file.name)
    if(filename.find(fileformat) != -1):
      SrcPath = file.path
      TgtPath =mntpath  + "/" + ENTITY_NAME + fileformat
      #TgtPath =mntpath  + "/" + ENTITY_NAME + "_"+  CurrentDate + "_" + str(i) + fileformat
      #print(SrcPath)
      #print(TgtPath)
      i = i + 1
      dbutils.fs.cp(SrcPath, TgtPath)

# COMMAND ----------

def checkdatecolforchars(dfRawData,col_name):
  myvar=dfRawData.select(col_name).rdd.flatMap(lambda x: x).collect()
  count=0
  for i in range(len(myvar)):
    myvar[i]=myvar[i].replace('-',"")
    myvar[i]=myvar[i].replace(":","")
    myvar[i]=myvar[i].replace(" ","")
    #print(myvar[i])
    if(myvar[i].isnumeric()):continue
    else:
      count=count+1  
  #print(count)
  if count==0:
    print('col_check Correct')
#     spark.sql("INSERT INTO ### values ('{0}','{1}','{2}','{3}','{4}',{5})".format(row.ENTITY_NAME,file.name,dataQualityRule,'PASSED','HeaderName Matched','current_timestamp()'))
  else:
    print("given column having data other than numerics")
    spark.sql("INSERT INTO {6} values ('{0}','{1}','{2}','{3}','{4}',{5})".format(row.ENTITY_NAME,file.name,'col_check','Failed','Value other than numerics','current_timestamp()'),dqtablename)

  return dfRawData

  

# COMMAND ----------

def diff(li1,li2):
  li_diff=[i for i in li1+li2 if i not in li1 or i not in li2]
  return li_diff

# COMMAND ----------

@udf
def get_YYYYMMDD (dateValue,dateFormat):
#   dateValue =str(dateValue)
#   dateFormat =str(dateFormat)
  if(dateFormat=="DD'YY"):
    mname=(dateValue.split("'")[-2])
    mnum = datetime.strptime(mname,'%b').month
    formattedmnum = f"{mnum:02}"
    year = (dateValue.split("'")[1])
    yearYYYY=datetime.strptime(year,'%y').year
    DateFormatted = str(yearYYYY) + str(formattedmnum) +'01'
    
  elif (dateFormat =='DD YYYY'):
    mname=(dateValue.split(" ")[-2])
    mnum = datetime.strptime(mname,'%B').month
    formattedmnum = f"{mnum:02}"
    year = (dateValue.split(" ")[1])
    DateFormatted = str(year) + str(formattedmnum) +'01'
  return DateFormatted

# COMMAND ----------

def Cast_Columns_diff_format(df_raw_data,sourceItemID,dateFormat):
  df_spec= spark.read.jdbc(jdbcUrl,"(select * from {1} SIS WHERE SIS.SOURCE_ITEM_ID ={0} ORDER BY CAST(DATA_ELEMENT_POSITION AS INT) OFFSET 0 ROWS)t".format(sourceItemID,specificationTableName))
  datafile_schema=df_raw_data.schema  
  for index,item in enumerate(datafile_schema.fields):
    for row in df_spec.rdd.collect():
      if(int(row.DATA_ELEMENT_POSITION) == index+1):
        df_raw_data_casted = df_raw_data
        if(str(row.DATA_TYPE) != str(item.dataType)+'()'):
          if(str(row.DATA_TYPE)== 'TimestampType()'):
            col_len=df_raw_data.select(max(length(col(item.name)))).rdd.max()[0]
            if(col_len==17):
              df_raw_data_casted = df_raw_data.withColumn(item.name,to_timestamp(col(item.name).cast("string"),'yyyyMMddHHmmssSSS'))
            else:
              df_raw_data_casted = df_raw_data.withColumn(item.name,unix_timestamp(col(item.name).cast("string"),dateFormat).cast("timestamp"))
          elif(str(row.DATA_TYPE) == 'DateType()'):
            df_raw_data_casted = df_raw_data.withColumn(item.name,to_date(unix_timestamp(col(item.name).cast("string"),dateFormat.replace('HHmmss','')).cast("timestamp")))
          elif(str(row.DATA_TYPE).startswith('Decimal')):
            df_raw_data_casted = df_raw_data.withColumn(item.name, col(item.name).cast(str.replace(row.DATA_TYPE,'Type','')))
          else:
            df_raw_data_casted = df_raw_data.withColumn(item.name, col(item.name).cast(str.replace(row.DATA_TYPE,'Type()','')))
          df_raw_data = df_raw_data_casted
        df_raw_data=df_raw_data.withColumnRenamed(item.name,row.ATTRIBUTE_NAME)
  return df_raw_data

# COMMAND ----------

def applycidmergelogic(enrichedTableName,source_item_name):
  import json
  params=df_params.select('DATA_SOURCE_PARAMETER_NAME').rdd.flatMap(lambda x:x).collect()
  source_item=source_item_name
  if 'cidmergelogic' in params:
    cid_cols =df_params.filter(df_params.DATA_SOURCE_PARAMETER_NAME=='cidmergelogic').select("DATA_SOURCE_PARAMETER_VALUE").rdd.max()[0]
    x=json.loads(cid_cols)
    cols=list(x.keys())
    if source_item in cols:
      #applying cid merge logic
      cid_column=x[source_item]
      print("cid column: ",cid_column)
      df_cnt = 1;
      cnt=1;
      df_affil_seed = spark.sql('''SELECT distinct A.{1} AS LSR_CID, B.WINNER_CID as WNR_CID,  0 AS DEPTH FROM  {0} A  INNER JOIN cid_mrg  B ON A.{1} = B.looser_cid  '''.format(enrichedTableName,cid_column))
      df_affil_seed.registerTempTable("vt_seed0")
      while (df_cnt != 0):
        tblnm = "vt_seed"+str(cnt-1);
        tblnm1 = "vt_seed"+str(cnt);
        df_affil_rec = spark.sql(''' SELECT distinct MRG.LSR_CID AS LSR_CID, B.WINNER_CID as WNR_CID, MRG.DEPTH+1 AS DEPTH  FROM {0} MRG  INNER JOIN  cid_mrg  B ON MRG.WNR_CID = B.looser_cid '''.format(tblnm))
        df_cnt=df_affil_rec.count()
        if(df_cnt!=0):
          df_affil_rec.registerTempTable(tblnm1);
        cnt = cnt + 1
      print(cnt)
      fin_query = ""
      a=0
      for a in range(0,(cnt - 1)):
        if(a == 0 ):
          fin_query = fin_query + "select distinct LSR_CID, WNR_CID, DEPTH from vt_seed"+str(a); 
        else: 
          fin_query = fin_query+" union select distinct LSR_CID, WNR_CID, DEPTH from vt_seed"+str(a);  
      print(fin_query)
      DF_CID_MERGE = spark.sql(fin_query)
      DF_CID_MERGE.createOrReplaceTempView('cid_merge')
      MRG_CNT=spark.sql('''select LSR_CID, WNR_CID from cid_merge where (LSR_CID,DEPTH) in (select LSR_CID, max(DEPTH) from cid_merge group by 1) ''').count()
      print(f'''Number of CID to be Merged:{MRG_CNT}''')

      spark.sql('''Merge into {0} p
      using  (select LSR_CID, WNR_CID from cid_merge where (LSR_CID,DEPTH) in (select LSR_CID, max(DEPTH) from cid_merge group by 1) ) c
      on p.{1}=c.LSR_CID
      when matched then update
      set p.{1}=c.WNR_CID
      '''.format(enrichedTableName,cid_column)) 

      # defaulting null CIDS
      spark.sql('''update {0} set {1}='7777777777' where {1} is null '''.format(enrichedTableName,cid_column))



# COMMAND ----------

def lengthpadding(dfRawData,source_item_name):
  import json
  params=df_params.select('DATA_SOURCE_PARAMETER_NAME').rdd.flatMap(lambda x:x).collect()
  if 'lengthpaddingreq' in params:
    data=df_params.filter(df_params.DATA_SOURCE_PARAMETER_NAME=='lengthpaddingreq').select("DATA_SOURCE_PARAMETER_VALUE").rdd.max()[0] 
    padding_info=json.loads(data)
    entity=source_item_name
    if entity in padding_info.keys():
      col_info=padding_info[entity]
      print(col_info)
      for col_name in col_info:
        print(col_name," ",col_info[col_name])
        dfRawData=dfRawData.withColumn(col_name,lpad(col_name,col_info[col_name],'0'))
      print("Length padding done for mentioned attributes accordingly")
  return dfRawData

# COMMAND ----------

def CreatingColumnsWithComments(table_name,df_raw_data,source_item_id):
  df_spec= spark.read.jdbc(jdbcUrl,"(select * from {1} SIS WHERE SIS.SOURCE_ITEM_ID ={0} ORDER BY CAST(DATA_ELEMENT_POSITION AS INT) OFFSET 0 ROWS)t".format(source_item_id,specificationTableName))
  spec_list=df_spec.select('ATTRIBUTE_NAME').rdd.flatMap(lambda x:x).collect()
  if 'HCP_ID' not in spec_list:
    df_new=df_raw_data.drop('HCP_ID').drop('Ingestion_Time').drop('File_Name').drop('MONTH_YEAR')
  else:
    df_new=df_raw_data.drop('Ingestion_Time').drop('File_Name').drop('MONTH_YEAR')
  col_list=df_new.dtypes
  if df_spec.count()>0:
    s=""
    for i in range(len(col_list)):
      col_names=col_list[i][0]
      col_dtype=col_list[i][1]
      col_desc=df_spec.select('ATTRIBUTE_DESCRIPTION').collect()[i][0]
      if col_desc != None:
        col_desc=col_desc.replace('\'','').replace('\"','')
      if col_desc == None:
        col_desc="none"
      s= s+col_names+' '+ col_dtype+" COMMENT '"+col_desc+"',"
    # print(s)
    s=s+ " File_Name String COMMENT '"+'Path of the raw File'+"',"+ " Ingestion_Time Timestamp COMMENT '"+'Time of data loaded'+"'"
    # print(s)
    print("CREATE TABLE IF NOT EXISTS {0} ({1}) USING DELTA".format(table_name,s))
    spark.sql("""CREATE TABLE IF NOT EXISTS {0} ({1}) USING DELTA LOCATION '{2}'""".format(table_name,s,foundation_path))
  

# COMMAND ----------

def CheckFileDuplication(df_file_check, targetTable): 
   if 'File_Name' in df_file_check.columns: 
       filenames = df_file_check.select('File_Name').distinct().rdd.flatMap(lambda x: x).collect() 
       for fileName in filenames: 
           try: 
               Actual_Fname=fileName.split('/')[-1] 
               query=f"DELETE FROM {targetTable} WHERE substring_index(file_name,'/',-1) =  '{Actual_Fname}' " 
               spark.sql(query) 
               print('File check for duplicaion done') 
           except Exception: 
               print(f'Error during file duplication check. It might be a new entity for ingestion.')

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
#Class to hold notebook information
class NotebookData:
   def __init__(self, path, timeout, parameters=None, retry=1):
       self.path = path
       self.timeout = timeout  # notebook-level timeout (seconds)
       self.parameters = parameters if parameters else {}
       self.retry = retry      # number of retries if notebook fails
#Function to run a notebook with retry logic
def submitNotebook(notebook):
   print(f"Running notebook {notebook.path} - {notebook.parameters}")
   try:
       if notebook.parameters:
           return dbutils.notebook.run(notebook.path, notebook.timeout, notebook.parameters)
       else:
           return dbutils.notebook.run(notebook.path, notebook.timeout)
   except Exception as e:
       if notebook.retry > 0:
           print(f"Retrying notebook {notebook.path}")
           notebook.retry -= 1
           return submitNotebook(notebook)  # retry
       else:
           raise e  # raise error if no retries left
#Function to run multiple notebooks in parallel
def parallelNotebooks(notebooks, numInParallel):
   """
   notebooks      : List of NotebookData objects
   numInParallel  : Maximum number of notebooks to run at the same time
   Returns a list of Future objects
   """
   with ThreadPoolExecutor(max_workers=numInParallel) as executor:
       futures = [executor.submit(submitNotebook, nb) for nb in notebooks]
   return futures

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException

# these defs were being used in foundation and enrichment when table is partitioned

def delta_exists(path):
  try:
    return DeltaTable.isDeltaTable(spark, path)
  except AnalysisException:
    return False

def write_delta_partitions(df, path, targetTable,source_item_id, mode):
  df_partition= spark.read.jdbc(jdbcUrl,"(select ATTRIBUTE_NAME,PARTITION_COLUMN_ORDERING from {1} SIS WHERE SIS.SOURCE_ITEM_ID ={0} and SIS.PARTITION_COLUMN_ORDERING > 0 )t".format(source_item_id,specificationTableName)).orderBy('PARTITION_COLUMN_ORDERING')
  partition_cols = [row["ATTRIBUTE_NAME"] for row in df_partition.collect()]
  
  if mode.lower() == 'overwrite':
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.sql.shuffle.partitions", "400")
    df = df.coalesce(200)
    df.write.format("delta").mode("overwrite").partitionBy(partition_cols).option("path", path).option("mergeSchema", "false").saveAsTable(targetTable)
  elif mode.lower() == 'append':
    df.write.format("delta").mode("append").partitionBy(partition_cols).option("path", path).option("mergeSchema", "true").saveAsTable(targetTable)

# COMMAND ----------

def CreatingColumnsWithCommentsPartitions(table_name,df_raw_data,source_item_id,deltatablepath):
  df_partition= spark.read.jdbc(jdbcUrl,"(select ATTRIBUTE_NAME,PARTITION_COLUMN_ORDERING from {1} SIS WHERE SIS.SOURCE_ITEM_ID ={0} and SIS.PARTITION_COLUMN_ORDERING > 0 )t".format(source_item_id,specificationTableName)).orderBy('PARTITION_COLUMN_ORDERING')

  partition_cols = [row["ATTRIBUTE_NAME"] for row in df_partition.collect()]
  partitions_cols_str = ",".join(partition_cols)

  df_spec= spark.read.jdbc(jdbcUrl,"(select * from {1} SIS WHERE SIS.SOURCE_ITEM_ID ={0} ORDER BY CAST(DATA_ELEMENT_POSITION AS INT) OFFSET 0 ROWS)t".format(source_item_id,specificationTableName))
  spec_list=df_spec.select('ATTRIBUTE_NAME').rdd.flatMap(lambda x:x).collect()

  if 'HCP_ID' not in spec_list:
    df_new=df_raw_data.drop('HCP_ID').drop('Ingestion_Time').drop('File_Name').drop('MONTH_YEAR')
  else:
    df_new=df_raw_data.drop('Ingestion_Time').drop('File_Name').drop('MONTH_YEAR')
  col_list=df_new.dtypes
  if df_spec.count()>0:
    s=""
    for i in range(len(col_list)):
      col_names=col_list[i][0]
      col_dtype=col_list[i][1]
      col_desc=df_spec.select('ATTRIBUTE_DESCRIPTION').collect()[i][0]
      if col_desc != None:
        col_desc=col_desc.replace('\'','').replace('\"','').replace(',','')
      if col_desc == None:
        col_desc="none"
      s= s+col_names+' '+ col_dtype+" COMMENT '"+col_desc+"',"
    # print(s)
    s=s+ " File_Name String COMMENT '"+'Path of the raw File'+"',"+ " Ingestion_Time Timestamp COMMENT '"+'Time of data loaded'+"'"
    # print(s)
    print("CREATE TABLE IF NOT EXISTS {0} ({1})USING DELTA PARTITIONED BY ({2}) LOCATION".format(table_name,s,partitions_cols_str))
    spark.sql("""CREATE TABLE IF NOT EXISTS {0} ({1}) USING DELTA PARTITIONED BY ({2}) LOCATION '{3}' """.format(table_name,s,partitions_cols_str,deltatablepath))
  