import sys
import boto3
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.window import Window
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from pyspark.sql.functions import col, row_number, to_date, lit, current_date
import hashlib

# Initialize Spark and Glue context
sc = SparkContext()
spark = SparkSession(sc)
glueContext = GlueContext(sc)

args = getResolvedOptions(sys.argv, ['bucket', 'key', 'format'])

job = Job(glueContext)
job.init('glue-job', args)

bucket_name = args['bucket']
file_key = args['key']
file_format = args['format']
dynamodb_table_name = "teamo-acc_master"
transaction_table = "ledger_txn_t6"
dynamodb_region = "us-east-1"

acc_master_dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="dynamodb",
    connection_options={
        "dynamodb.input.tableName": dynamodb_table_name,
        "dynamodb.region": dynamodb_region,
        "dynamodb.throughput.read.percent": "1.0",
        "dynamodb.splits": "100"
    }
)

s3_uri = f"s3://{bucket_name}/{file_key}"
failure_output_path = f"s3://{bucket_name}/failed_transactions/failed_{file_key.split('/')[-1].split('.')[0]}"

acc_master_df = acc_master_dynamic_frame.toDF()

account_numbers = acc_master_df.select("acc_name", "acc_no").rdd.collectAsMap()

furniture_acc_no = account_numbers.get("furniture_sale_amt")
tools_acc_no = account_numbers.get("tools_sale_amt")
gst_acc_no = account_numbers.get("gst")
vat_acc_no = account_numbers.get("vat")
excise_acc_no = account_numbers.get("excise_duty")
custom_duty_acc_no = account_numbers.get("custom_duty")

account_mapping = {
    'furniture_sale_amt': furniture_acc_no,
    'tools_sale_amt': tools_acc_no,
    'gst': gst_acc_no,
    'vat': vat_acc_no,
    'excise_duty': excise_acc_no,
    'custom_duty': custom_duty_acc_no
}

def generate_voucher_code(tran_ref_id):
    hash_object = hashlib.md5(tran_ref_id.encode())
    return hash_object.hexdigest()[:10].upper()

dynamo_client = boto3.client('dynamodb', region_name=dynamodb_region)

def get_max_txn_id():
    response = dynamo_client.scan(
        TableName=transaction_table,
        ProjectionExpression="txn_id",
        Select="SPECIFIC_ATTRIBUTES"
    )
    txn_ids = [item['txn_id']['N'] for item in response.get('Items', [])]
    return max([int(id) for id in txn_ids], default=0)

max_txn_id = get_max_txn_id()

def write_failed_records(failed_df, failure_output_path):
    failed_df.write.option("header", "true").csv(failure_output_path)
    print(f"Failed records written to {failure_output_path}")

if file_format == "parquet":
    df = spark.read.parquet(s3_uri)
    valid_df = df.filter(
        (col('tdate').isNotNull()) & 
        (col('trn_amount').isNotNull()) & 
        (col('trn_amount') > 0)
    )
    invalid_df = df.subtract(valid_df)
    if invalid_df.count() > 0:
        print(f"Error: {invalid_df.count()} records are invalid.")
        invalid_df.write.option("header", "true").csv(failure_output_path)
    
    window_spec = Window.orderBy("txn_date")

    ledger_df = valid_df.select(
        col('trn_ref_id').alias("source_system_txn_id"),
        col('code').alias('voucher_code'),
        lit('C').alias('txn_type'),
        to_date(col('tdate'), 'dd-MMM-yy').alias('txn_date'),
        lit(furniture_acc_no).alias('acc_no'),
        col('trn_amount').alias('txn_amt'),
        lit(2).alias("source_system_id")
    ).union(
        valid_df.select(
            col('trn_ref_id').alias('source_system_txn_id'),
            col('code').alias('voucher_code'),
            lit('C').alias('txn_type'),
            to_date(col('tdate'), 'dd-MMM-yy').alias('txn_date'),
            lit(gst_acc_no).alias('acc_no'),
            col('vat').alias('txn_amt'),
            lit(2).alias('source_system_id')
        )
    ).union(
        valid_df.select(
            col('trn_ref_id').alias('source_system_txn_id'),
            col('code').alias('voucher_code'),
            lit('C').alias('txn_type'),
            to_date(col('tdate'), 'dd-MMM-yy').alias('txn_date'),
            lit(custom_duty_acc_no).alias('acc_no'),
            col('excise_duty').alias('txn_amt'),
            lit(2).alias('source_system_id')
        )
    )

    ledger_df_with_continuous_txn_id = ledger_df.withColumn(
        "txn_id", row_number().over(window_spec) + max_txn_id
    )

    ledger_dynamic_frame = DynamicFrame.fromDF(ledger_df_with_continuous_txn_id, glueContext, "ledger_dynamic_frame")

    try:
        glueContext.write_dynamic_frame.from_options(
            frame=ledger_dynamic_frame,
            connection_type="dynamodb",
            connection_options={
                "dynamodb.output.tableName": transaction_table,
                "dynamodb.region": dynamodb_region
            }
        )
        print("GST and Custom Duty Transactions added successfully.")
    except Exception as e:
        print(f"Error writing to DynamoDB: {e}")
        write_failed_records(ledger_df_with_continuous_txn_id, failure_output_path)

job.commit()