from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SalesTransform").getOrCreate()

df = spark.read.csv("../data/cleaned_sales_data.csv", header=True, inferSchema=True)

summary = df.groupBy("category").sum("total_amount")

summary.show()
