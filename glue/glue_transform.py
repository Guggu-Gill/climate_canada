
#RUN THIS CODE IN VISUAL EDITOR OF AWS GLUE

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


output_path='s3://output_bucket'
input_path = 's3://input_bucket'
# Script generated for node Amazon S3
df = spark.read.option("header", "true").csv(f"{input_path}/*.csv")


year_diff = df.groupBy('StnID').agg(
    F.min('Year').alias('min_year'), 
    F.max('Year').alias('max_year')
)

year_diff = year_diff.withColumn('year_range', F.col('max_year') - F.col('min_year'))

stn_with_max_diff_df = year_diff.filter(F.col('year_range') >= 60)

stn_with_max_diff = stn_with_max_diff_df.select('StnID').rdd.flatMap(lambda x: x).collect()

df_filtered = df.filter(F.col('StnID').isin(stn_with_max_diff))

df_filtered = df_filtered.filter("StnID != 2315 AND StnID != 1650")

df_filtered.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)




job.commit()