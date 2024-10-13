#RUN THIS CODE IN VISUAL EDITOR OF AWS GLUE



import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col,regexp_extract,input_file_name


args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bucket_name = 's3://input_bucket'
output_path = 's3://output_bucket'


feat_to_keep=["Date/Time","Year","Month","Day","Max Temp (°C)","Min Temp (°C)","Mean Temp (°C)","Heat Deg Days (°C)","Cool Deg Days (°C)","Total Rain (mm)","Total Snow (cm)","Total Precip (mm)","Snow on Grnd (cm)"]


data_frame = spark.read.option("header", "true").csv(f"{bucket_name}/climate-canada/*/*.csv")

data_frame = data_frame.withColumn("StnID", regexp_extract(input_file_name(), '([^/]+)/[^/]+$', 1))

consolidated_df = data_frame.select(*feat_to_keep,"StnID").dropna()

consolidated_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

job.commit()