import json
import os
import sharedutils
import time


def lambda_handler(event, context):

    # Environment variables
    database_name = os.getenv('DatabaseName')
    table_name = os.getenv('TableName')
    fixity_output_bucket_name = os.getenv('FixityOutputBucket')
    query_result_bucket_name = os.getenv('ResultBucket')
    state_machine_arn = os.getenv('StateMachineArn')
    dayPeriod = int(os.getenv('DayPeriod'))

    s3_output = f's3://{query_result_bucket_name}/results/'
    startDate = sharedutils.getDateFromDay(dayPeriod)
    endDate = sharedutils.getDateFromDay(0)

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

    queryResponse = sharedutils.execute_query(
        query=query,
        database=database_name,
        s3_output=s3_output)
    results = sharedutils.get_query_result(queryResponse["QueryExecutionId"])
    outputBucket = fixity_output_bucket_name

    taskResponse = ""

    if len(results) == 0:
        taskResponse = "Query Athena is failed"
    else:
        for x in range(1, len(results["Rows"])):
            task_json = sharedutils.create_steps_task_json(
                results["Rows"][x]["Data"],
                outputBucket)
            print("Task:" + task_json)
            taskResponse = "State machine: " + \
                str(sharedutils.execute_step_functions(state_machine_arn, task_json))
            time.sleep(2)

    print(taskResponse)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": taskResponse,
        }),
    }
