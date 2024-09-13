import sys
import boto3

client = boto3.client('athena')

SOURCE_TABLE_NAME = 'weather_meteo_project_em1'
NEW_TABLE_NAME = 'parquet-table-em1'
NEW_TABLE_S3_BUCKET = 'parquet-bucket-em-proj-1/'
MY_DATABASE = 'em_proj_de_aug_2024_db'
QUERY_RESULTS_S3_BUCKET = 'em-query-result-storage-from-athena-de-proj-aug-2024/'

# Refresh the table
queryStart = client.start_query_execution(
    QueryString = f"""
    CREATE TABLE {NEW_TABLE_NAME} WITH
    (external_location='{NEW_TABLE_S3_BUCKET}',
    format='PARQUET',
    write_compression='SNAPPY',
    partitioned_by = ARRAY['yr_mo_partition'])
    AS

    SELECT
        latitude
        ,longitude
        ,temp AS temp_F
        ,(temp - 32) * (5.0/9.0) AS temp_C
        ,row_ts
        ,time
        ,SUBSTRING(time,1,7) AS yr_mo_partition
    FROM "{MY_DATABASE}"."{SOURCE_TABLE_NAME}"

    ;
    """,
    QueryExecutionContext = {
        'Database': f'{MY_DATABASE}'
    }, 
    ResultConfiguration = { 'OutputLocation': f'{QUERY_RESULTS_S3_BUCKET}'}
)

# list of responses
resp = ["FAILED", "SUCCEEDED", "CANCELLED"]

# get the response
response = client.get_query_execution(QueryExecutionId=queryStart["QueryExecutionId"])

# wait until query finishes
while response["QueryExecution"]["Status"]["State"] not in resp:
    response = client.get_query_execution(QueryExecutionId=queryStart["QueryExecutionId"])
    
# if it fails, exit and give the Athena error message in the logs
if response["QueryExecution"]["Status"]["State"] == 'FAILED':
    sys.exit(response["QueryExecution"]["Status"]["StateChangeReason"])
