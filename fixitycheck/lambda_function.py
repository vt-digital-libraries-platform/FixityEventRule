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


def execute_step_functions(stepMachineArn, inputJson):

    client = boto3.client('stepfunctions')
    taskname = "task-" + str(uuid.uuid4())

    response = client.start_execution(
        stateMachineArn=stepMachineArn,
        name=taskname,
        input=inputJson
    )

    return response


def lambda_handler(event, context):

    # Environment variables
    database_name = os.getenv('DatabaseName')
    table_name = os.getenv('TableName')
    fixity_output_bucket_name = os.getenv('FixityOutputBucket')
    query_result_bucket_name = os.getenv('ResultBucket')
    state_machine_arn = os.getenv('StateMachineArn')
    dayPeriod = int(os.getenv('DayPeriod'))

    s3_output = f's3://{query_result_bucket_name}/results/'
    startDate = getDateFromDay(dayPeriod)
    endDate = getDateFromDay(0)

    query = """
      SELECT bucket, key
      FROM %s
      WHERE key NOT IN
        (SELECT key
        FROM %s
        WHERE timestamp
          BETWEEN CAST('%s' AS DATE)
            AND CAST('%s' AS DATE) ) LIMIT 1
    """ % (table_name, table_name, startDate, endDate)

    queryResponse = execute_query(
        query=query,
        database=database_name,
        s3_output=s3_output)
    results = get_query_result(queryResponse["QueryExecutionId"])
    outputBucket = fixity_output_bucket_name

    taskResponse = ""

    if len(results) == 0:
        taskResponse = "Query Athena is failed"
    else:
        for x in range(1, len(results["Rows"])):
            task_json = create_steps_task_json(
                results["Rows"][x]["Data"],
                outputBucket)
            print("Task:" + task_json)
            taskResponse = "State machine: " + \
                str(execute_step_functions(state_machine_arn, task_json))
            time.sleep(2)

    print(taskResponse)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": taskResponse,
        }),
    }
