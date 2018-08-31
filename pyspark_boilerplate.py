# PySpark Boilerplate

# Spark Configuration

# change the default max result size (ng)
spark.driver.maxResultSize 220g

# enable compression
spark.rdd.compress true
spark.sql.inMemoryColumnarStorage.compressed true

# parallelize the cluster to the number of clusters (n)
spark.default.parallelism (n)

# SparkSession Module to start a module
from pyspark.sql import SparkSession


# Windowing functions
from pyspark.sql.window import Window


# Row and Column Functions to manipulate Spark dataframes
from pyspark.sql import Row, Column


# Sql Types for objects
from pyspark.sql.types import *

# I tend to import Functions as F, this is good practice because if you
# import pyspark.sql.functions that have the same name as the python functions
# (eg. mean, sum, etc) it might overwrite the native python. however, I also
# import some functions that are not native to python with just their names
# for not having to type "F." all the time
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf, from_unixtime, unix_timestamp

# set saving to snappy parquet
sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")

# Start SparkSession
spark = (SparkSession.builder.appName("session_name")
                            .enableHiveSupport()
                            .getOrCreate())

# Mounting an S3 bucket:
ACCESS_KEY = "access_key"
# # Encode the Secret Key as that can contain "/"
SECRET_KEY = "".replace("/", "%2F")
AWS_BUCKET_NAME = "bucket_name"
MOUNT_NAME = "mount_name"

# dbutils.fs.mount("s3a://%s:%s@%s" % (ACCESS_KEY, SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)
display(dbutils.fs.ls("/mnt/%s/raw" % MOUNT_NAME))


# reading CSVs as dataframes
bucket_path = '/mnt/bucket_path'
df_name = (spark.read.format('csv').options(header='true', inferSchema='true')
                                   .load(bucket_path + "csv_name.csv"))

# reading parquet as dataframe
df_name = spark.read.parquet(bucket_path + 'parquet_name')


# saving parque
df.write.mode('overwrite').parquet('')

#window example
# windows can be created to implement functions over defined windows in a
# dataset, in some ways you can think of them as an alternative to groupbys,
# whereas groupbys require aggregating the data, windowing function duplicates
# the results and appends them to each observation in that window
windowSpecRankMax = Window.partitionBy(['id', 'date', 'value']).orderBy(session_df_all['value'].desc())
windowSpecRankEarliest = Window.partitionBy(['id', 'date', 'value']).orderBy(session_df_all['date'])

platform_df = df.select('id', 'date', 'value',
               F.rank().over(windowSpecRankMax).alias('rank_value'),
               F.row_number().over(windowSpecRankMax).alias('row_value'),
               F.rank().over(windowSpecRankEarliest).alias('rank_earliest'))
