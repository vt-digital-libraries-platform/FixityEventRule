import boto3
import json
import os
import time
import uuid

from datetime import datetime, timedelta

def getDateFromDay(days):

    from_date = datetime.now() - timedelta(days=days)
    return from_date.strftime('%Y-%m-%d')

def execute_query(query, database, s3_output):
    """Execute the Athena query"""

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
        print('Query status: ' + status)

        if (status == 'FAILED') or (status == 'CANCELLED'):
            return result_data

        elif status == 'SUCCEEDED':
            response_get_query_details['QueryExecution']['ResultConfiguration']['OutputLocation']

            response_query_result = client.get_query_results(
                QueryExecutionId=executionId
            )
            result_data = response_query_result['ResultSet']
            return result_data
        else:
            time.sleep(2)

    return result_data

def list_query_results(inputData):

    record = list(map(get_value_from_list, inputData))
    result = '"S3 Bucket": "%s", "Filename": "%s"' % (
        record[0], record[1])

    return result

def get_value_from_list(inputData):
    return inputData["VarCharValue"]


def lambda_handler(event, context):

    # Environment variables
    snstopicarn = os.getenv('FixitySNS')
    database_name = os.getenv('DatabaseName')
    table_name = os.getenv('TableName')
    query_result_bucket_name = os.getenv('ResultBucket')
    
    s3_output = f's3://{query_result_bucket_name}/results/'
    startDate = getDateFromDay(2)
    endDate = getDateFromDay(0)
    
    query = """
      SELECT bucket, key
      FROM %s
      WHERE timestamp
        BETWEEN CAST('%s' AS DATE)
            AND CAST('%s' AS DATE)
            AND comparedresult = 'MATCHED'
    """ % (table_name, startDate, endDate)

    print(query)

    queryResponse = execute_query(
        query=query,
        database=database_name,
        s3_output=s3_output)
    results = get_query_result(queryResponse["QueryExecutionId"])

    message = ""

    if len(results) == 0:
        response = "Query Athena is failed"
        print(response)
    else:
        totalResult = str( len(results["Rows"]) -1 )
        print("Total: " + totalResult)
        message = "Total: " + totalResult + "\n"

        for x in range(1, len(results["Rows"])):
            queryResult = list_query_results(results["Rows"][x]["Data"])
            print(queryResult)
            message = message + queryResult + "\n"

    client = boto3.client('sns')

    response = client.publish(
        TargetArn=snstopicarn,
        Message=message,
        Subject='Fixity Result'
    )

    print("SNS test")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": response,
        }),
    }
