import sys
import logging
import os
import boto3
import uuid
import json
from datetime import datetime

from pyspark.context import SparkContext
from pyspark.sql.functions import col, year, month, unix_timestamp, count, avg, sum, round
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

os.environ["SPARK_VERSION"] = "3.3"

from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult


MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger("TLC Data Transformation")
logger.setLevel(logging.INFO)

def get_year_month_partitions(object_key: str):
    import re
    match = re.search(r'year=(\d{4})/month=(\d{2})', object_key)
    if match:
        partition_year = match.group(1)
        partition_month = match.group(2)
    else:
        raise ValueError(
            "No se encontró la partición year=YYYY/month=MM en el objeto S3")
    return partition_year, partition_month

def get_data_from_s3(input_path: str, reference_file_path: str):
    df_trips = spark.read.option("header", "true").parquet(input_path)
    df_boroughs = spark.read.option("header", "true").csv(reference_file_path)

    logger.info(f"df_trips shape: {df_trips.count()}")
    logger.info(f"df_boroughs shape: {df_boroughs.count()}")

    return df_trips, df_boroughs

def filter_data_by_date(df_trips, partition_year: str, partition_month: str):
    df_trips_filtered = df_trips\
        .filter(
            (year(col("lpep_pickup_datetime")) == int(partition_year)) & 
            (month(col("lpep_pickup_datetime")) == int(partition_month)) &
            (col("DOLocationID").isNotNull()) &
            (col("PULocationID").isNotNull())
        )
    logger.info(f"df_trips_filtered shape: {df_trips_filtered.count()}")

    return df_trips_filtered

def get_pickup_dropoff(file_type: str):
    if file_type in ("green-trip-data", 'yellow-trip-data'):
        _pickup = "lpep_pickup_datetime"
        _dropoff = "lpep_dropoff_datetime"
    else:
        _pickup = "pickup_datetime"
        _dropoff = "dropoff_datetime"

    return _pickup, _dropoff

def calculate_trip_duration(df_trips_filtered, pickup: str, dropoff: str):
    df_trips_filtered = df_trips_filtered\
        .withColumn(
            "trip_duration", (
                unix_timestamp(col(dropoff)) - unix_timestamp(col(pickup))
            ) / 60
        )
    logger.info(f"df_trips_filtered shape: {df_trips_filtered.count()}")

    return df_trips_filtered

def join_trips_with_borough(df_trips_filtered, df_boroughs):
    df_with_borough = df_trips_filtered\
        .join(
            df_boroughs, df_trips_filtered["PULocationID"] == df_boroughs["LocationID"], "left")\
        .select(
            df_trips_filtered["*"], col("Borough").alias("PUBorough"))

    df_with_borough = df_with_borough\
        .join(
            df_boroughs, df_trips_filtered["DOLocationID"] == df_boroughs["LocationID"], "left")\
        .select(
            df_with_borough["*"], col("Borough").alias("DOBorough"))

    logger.info(f"df_with_borough shape: {df_with_borough.count()}")

    return df_with_borough

def aggregate_trips_by_borough(df_with_borough):

    df_trips_aggregated = df_with_borough\
        .groupBy("PUBorough", "DOBorough")\
        .agg(
            count("VendorID").alias("total_trips"),
            round(avg("trip_distance"), 2).alias("avg_trip_distance"),
            round(avg("trip_duration"), 2).alias("avg_trip_duration"),
            round(avg("passenger_count"), 2).alias("avg_passenger_count"),
            round(avg("total_amount"), 2).alias("avg_total_amount"),
            round(avg("tip_amount"), 2).alias("avg_tip_amount"),
            round(avg("fare_amount"), 2).alias("avg_fare_amount"),
            round(avg("extra"), 2).alias("avg_extra"),
            round(sum("trip_distance"), 2).alias("total_trip_distance"),
            round(sum("trip_duration"), 2).alias("total_trip_duration")
        )
    logger.info(f"df_trips_aggregated shape: {df_trips_aggregated.count()}")

    return df_trips_aggregated

def load_data_to_s3(df_trips_aggregated, output_path):
    df_trips_aggregated\
        .write\
        .mode("overwrite")\
        .parquet(output_path)

    logger.info("Data Loaded")

def upload_validation_to_dynamoDB(
    verificationResult_df, dynamodb_validation_table: str, date_now: str):
    dynamodb = boto3.client('dynamodb', region_name='us-east-2')

    for row in verificationResult_df.collect():
        validation_id = str(uuid.uuid4())
        item = {
            'ValidationID': {'S': validation_id},
            'Timestamp': {'S': date_now},
            'Check': {'S': row['check']},
            'Constraint': {'S': row['constraint']},
            'Status': {'S': row['constraint_status']},
            'Message': {'S': row['constraint_message'] if 'constraint_message' in row else ''}
        }

        dynamodb.put_item( TableName=dynamodb_validation_table, Item=item )

    logger.info("Verification save in DynamoDB")

def upload_validation_to_s3(verificationResult_df, s3_validation_output_path: str):
    verificationResult_df\
        .coalesce(1)\
        .write.mode("overwrite")\
        .csv(s3_validation_output_path, header=True)

    logger.info("Verification save in S3")

def run_deequ_constrains_validations(
        spark, df_with_borough, pickup: str, dropoff: str,
        dynamodb_validation_table: str, s3_validation_output_path: str
    ):

    key_columns = [pickup, dropoff, "PULocationID", "DOLocationID"]
    date_now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

    check_null_values = Check(
        spark_session=spark, level=CheckLevel.Error,
        description="Data Validation - Nullity Checks") \
            .isComplete(pickup) \
            .isComplete(dropoff) \
            .isComplete("PULocationID") \
            .isComplete("DOLocationID")

    check_borough_within_nyc_boundaries = Check(
        spark_session=spark, level=CheckLevel.Warning,
        description="Data Validation - NYC Borough Boundaries") \
        .isContainedIn("PUBorough", ['EWR', 'Queens', 'Bronx', 'Manhattan', 'Staten Island', 'Brooklyn']) \
        .isContainedIn("DOBorough", ['EWR', 'Queens', 'Bronx', 'Manhattan', 'Staten Island', 'Brooklyn'])

    check_duplicates = Check(
        spark_session=spark, level=CheckLevel.Warning,
        description="Data Validation - Duplicates") \
            .hasUniqueness(key_columns, assertion=lambda x: x==1)

    # Run the verification
    checkResult = VerificationSuite(spark) \
        .onData(df_with_borough) \
        .addCheck(check_null_values) \
        .addCheck(check_borough_within_nyc_boundaries) \
        .addCheck(check_duplicates) \
        .run()

    # .addCheck(check_borough_within_nyc_boundaries) \
    # .addCheck(check_duplicates) \

    logger.info("Verification ended")

    verificationResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)

    upload_validation_to_dynamoDB(
        verificationResult_df, dynamodb_validation_table, date_now)
    upload_validation_to_s3(verificationResult_df, s3_validation_output_path)

    if checkResult.status == "Success":
        logger.info("Data Validation Passed!")
    else:
        logger.info("Data Validation Failed!")

def run_etl(
        spark, input_path: str, output_path: str, reference_file_path: str, dynamodb_validation_table: str,
        s3_validation_output_path:str, partition_year: str, partition_month: str, file_type: str
    ):
    df_trips, df_boroughs = get_data_from_s3(input_path, reference_file_path)
    df_trips_filtered = filter_data_by_date(df_trips, partition_year, partition_month)
    pickup, dropoff = get_pickup_dropoff(file_type)
    df_trips_filtered = calculate_trip_duration(df_trips_filtered, pickup, dropoff)
    df_with_borough = join_trips_with_borough(df_trips_filtered, df_boroughs)

    run_deequ_constrains_validations(
        spark, df_with_borough, pickup, dropoff,
        dynamodb_validation_table, s3_validation_output_path
    )

    logger.info(f"Test: {df_with_borough.count()}")

    df_trips_aggregated = aggregate_trips_by_borough(df_with_borough)
    load_data_to_s3(df_trips_aggregated, output_path)


args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bucket_name', 'object_key', 'reference_file_path', 'object_file'])
bucket_name = args['bucket_name']
object_key = args['object_key']
object_file = args['object_file']
reference_file_path = args['reference_file_path']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


partition_year, partition_month = get_year_month_partitions(object_key)
file_type = object_key.split('/')[1]
input_path = f"s3://{bucket_name}/{object_key}/{object_file}"
output_path = f"s3://{bucket_name}/stage/{file_type}/year={partition_year}/month={partition_month}/"
s3_validation_output_prefix = f"deequ-validation-results/year={partition_year}/month={partition_month}"
s3_validation_output_path = f"s3://{bucket_name}/{s3_validation_output_prefix}/"
dynamodb_validation_table = 'tlc-trip-data-etl-data-validation'


run_etl(
    spark, input_path, output_path, reference_file_path, dynamodb_validation_table,
    s3_validation_output_path, partition_year, partition_month, file_type
)

spark.sparkContext.stop()
job.commit()
