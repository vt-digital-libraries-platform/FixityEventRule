import boto3
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