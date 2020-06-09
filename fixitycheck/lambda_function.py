import boto3
import json
import os
import time


def execute_query(query, database, s3_output):
    """Execute the Athena query"""
    print("Executing query: %s" % (query))
    client = boto3.client('athena')
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': s3_output,
        }
    )
    print('Execution ID: ' + response['QueryExecutionId'])
    return response


def get_query_result(executionId):

    client = boto3.client('athena')
    status = 'RUNNING'
    iterations = 5
    result_data = {}

    while (iterations > 0):
        iterations = iterations - 1
        response_get_query_details = client.get_query_execution(
            QueryExecutionId=executionId
        )

        status = response_get_query_details['QueryExecution']['Status']['State']
        print(status)

        if (status == 'FAILED') or (status == 'CANCELLED'):
            return result_data

        elif status == 'SUCCEEDED':
            location = response_get_query_details['QueryExecution']['ResultConfiguration']['OutputLocation']

            response_query_result = client.get_query_results(
                QueryExecutionId=executionId
            )
            result_data = response_query_result['ResultSet']
            return result_data
        else:
            time.sleep(2)

    return result_data


def create_steps_task_json(inputData, outputBucket):

    record = list(map(get_value_from_list, inputData))
    task_json = '{"Bucket": "%s", "Key": "%s", "OutputBucket": "%s"}' % (
        record[0], record[1], outputBucket)

    return task_json


def get_value_from_list(inputData):
    return inputData["VarCharValue"]



def lambda_handler(event, context):

    # Environment variables
    database_name = os.getenv('DatabaseName')
    table_name = os.getenv('TableName')
    fixity_output_bucket_name = os.getenv('FixityOutputBucket')
    query_result_bucket_name = os.getenv('ResultBucket')

    # database_name = "default"
    # table_name = "output"
    # query_result_bucket_name = "fixity-query-result"
    s3_output = f's3://{query_result_bucket_name}/results/'

    # update to calculate 90 days
    query = """
          SELECT bucket, key
          FROM output
          WHERE timestamp
              BETWEEN CAST('2020-05-01' AS DATE)
                  AND CAST('2020-06-01' AS DATE)
                  AND key NOT IN
              (SELECT key
              FROM output
              WHERE timestamp >= CAST('2020-06-01' AS DATE) )
        """

    # query = """ SELECT bucket, key
    #         FROM output
    #         WHERE timestamp >= CAST('2020-06-01' AS DATE) limit 3
    #         """

    queryResponse = execute_query(
        query=query,
        database=database_name,
        s3_output=s3_output)
    results = get_query_result(queryResponse["QueryExecutionId"])
    outputBucket = fixity_output_bucket_name

    for x in range(1, len(results["Rows"])):
        # execute steps functions
        print(create_steps_task_json(results["Rows"][x]["Data"], outputBucket))
        # update lambda timout limit
        # sleep 2

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Task executed.",
        }),
    }
