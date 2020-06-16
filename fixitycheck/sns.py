import boto3
import json
import os
import sharedutils

from datetime import datetime, timedelta


def lambda_handler(event, context):

    # Environment variables
    snstopicarn = os.getenv('FixitySNS')
    database_name = os.getenv('DatabaseName')
    table_name = os.getenv('TableName')
    query_result_bucket_name = os.getenv('ResultBucket')

    s3_output = f's3://{query_result_bucket_name}/results/'
    startDate = sharedutils.getDateFromDay(1)
    endDate = sharedutils.getDateFromDay(0)

    query = """
      SELECT bucket, key
      FROM %s
      WHERE timestamp
        BETWEEN CAST('%s' AS DATE)
            AND CAST('%s' AS DATE)
            AND comparedresult = 'MATCHED'
    """ % (table_name, startDate, endDate)

    print(query)

    queryResponse = sharedutils.execute_query(
        query=query,
        database=database_name,
        s3_output=s3_output)
    results = sharedutils.get_query_result(queryResponse["QueryExecutionId"])

    message = ""

    if len(results) == 0:
        response = "Query Athena is failed"
        print(response)
    else:
        totalResult = str(len(results["Rows"]) - 1)
        print("Total: " + totalResult)
        message = "Total: " + totalResult + "\n"

        for x in range(1, len(results["Rows"])):
            queryResult = sharedutils.list_query_results(
                results["Rows"][x]["Data"])
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
