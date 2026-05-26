import os
import urllib.request
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, window, sum, max, min, count, date_format, round
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# 1. Windows PySpark Environment Setup
os.environ["HADOOP_USER_NAME"] = "admin"

current_dir = os.getcwd()
hadoop_home = os.path.join(current_dir, "hadoop")
hadoop_bin = os.path.join(hadoop_home, "bin")
os.makedirs(hadoop_bin, exist_ok=True)

os.environ["HADOOP_HOME"] = hadoop_home
os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

winutils_path = os.path.join(hadoop_bin, "winutils.exe")
hadoop_dll_path = os.path.join(hadoop_bin, "hadoop.dll")

try:
    if not os.path.exists(winutils_path):
        print("Downloading winutils.exe...")
        urllib.request.urlretrieve("https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin/winutils.exe", winutils_path)
    if not os.path.exists(hadoop_dll_path):
        print("Downloading hadoop.dll...")
        urllib.request.urlretrieve("https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin/hadoop.dll", hadoop_dll_path)
except Exception as e:
    print("Warning: Could not download Hadoop binaries automatically.")

# 2. Initialize Spark Session and configure MinIO connection with strict numeric timeouts
spark = SparkSession.builder \
    .appName("Mohaymen-Spark-Streaming") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "super_secret_password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000") \
    .config("spark.hadoop.fs.s3a.connection.acquisition.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.connection.idle.time", "60000") \
    .config("spark.hadoop.fs.s3a.connection.request.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "200000") \
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000") \
    .config("spark.hadoop.fs.s3a.connection.ttl", "86400000") \
    .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400") \
    .config("spark.hadoop.fs.s3a.assumed.role.session.duration", "3600") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 3. Define static schemas
sms_schema = StructType([
    StructField("ROAMSTATE_519", StringType(), True),
    StructField("CUST_LOCAL_START_DATE_15", StringType(), True),
    StructField("CDR_ID_1", StringType(), True),
    StructField("CDR_SUB_ID_2", StringType(), True),
    StructField("CDR_TYPE_3", StringType(), True),
    StructField("SPLIT_CDR_REASON_4", StringType(), True),
    StructField("RECORD_DATE", StringType(), True),
    StructField("PAYTYPE_515", IntegerType(), True),
    StructField("DEBIT_AMOUNT_42", DoubleType(), True),
    StructField("SERVICEFLOW_498", StringType(), True),
    StructField("EVENTSOURCE_CATE_17", StringType(), True),
    StructField("USAGE_SERVICE_TYPE_19", StringType(), True),
    StructField("SPECIALNUMBERINDICATOR_534", StringType(), True),
    StructField("BE_ID_30", StringType(), True),
    StructField("CALLEDPARTYIMSI_495", StringType(), True),
    StructField("CALLINGPARTYIMSI_494", StringType(), True)
])

ref_schema = StructType([
    StructField("PayType", IntegerType(), True),
    StructField("value", StringType(), True)
])

# 4. Read data
ref_df = spark.read.csv("data/ref/", header=True, schema=ref_schema)

stream_df = spark.readStream \
    .schema(sms_schema) \
    .option("header", "true") \
    .csv("data/REF_SMS/")

# 5. Preprocessing
processed_stream = stream_df \
    .withColumn("timestamp", to_timestamp(col("RECORD_DATE"), "yyyy/MM/dd HH:mm:ss")) \
    .withColumn("revenue_toman", col("DEBIT_AMOUNT_42") / 10000) \
    .join(ref_df, stream_df["PAYTYPE_515"] == ref_df["PayType"], "left")

# 6. Aggregations
daily_agg = processed_stream \
    .groupBy(window(col("timestamp"), "1 day")) \
    .agg(sum("revenue_toman").alias("total_revenue"))

quarter_agg = processed_stream \
    .groupBy(window(col("timestamp"), "15 minutes"), col("value").alias("Pay type")) \
    .agg(
        sum("revenue_toman").alias("revenue"),
        max("revenue_toman").alias("max_revenue"),
        min("revenue_toman").alias("min_revenue"),
        count("*").alias("Record_Count")
    )

# 7. Output functions
def write_daily_to_minio(batch_df, batch_id):
    out_df = batch_df.select(
        date_format(col("window.start"), "yyyy/MM/dd").alias("Date"),
        round(col("total_revenue"), 2).alias("Daily_Revenue_Toman")
    )
    out_df.coalesce(1).write.mode("overwrite").csv("s3a://reports/report1_daily", header=True)

def write_quarterly_to_minio(batch_df, batch_id):
    base_df = batch_df.select(
        date_format(col("window.start"), "HH:mm:ss yyyy/MM/dd").alias("RECORD_DATE"),
        col("Pay type"),
        col("revenue"),
        col("max_revenue"),
        col("min_revenue"),
        col("Record_Count")
    )
    base_df.persist()
    
    base_df.select("RECORD_DATE", "Pay type", round("revenue", 2).alias("revenue")) \
        .coalesce(1).write.mode("overwrite").csv("s3a://reports/report2_revenue", header=True)
        
    base_df.select("RECORD_DATE", "Pay type", round("max_revenue", 2).alias("max_revenue"), round("min_revenue", 2).alias("min_revenue")) \
        .coalesce(1).write.mode("overwrite").csv("s3a://reports/report3_minmax", header=True)
        
    base_df.select("RECORD_DATE", "Pay type", "Record_Count", round("revenue", 2).alias("revenue")) \
        .coalesce(1).write.mode("overwrite").csv("s3a://reports/report4_full", header=True)
        
    base_df.unpersist()

# 8. Start queries
query1 = daily_agg.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_daily_to_minio) \
    .start()

query2 = quarter_agg.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_quarterly_to_minio) \
    .start()

print("Spark Streaming is processing data and sending reports to MinIO...")
print("Check your MinIO dashboard. Press Ctrl+C to stop.")

query1.awaitTermination()
query2.awaitTermination()