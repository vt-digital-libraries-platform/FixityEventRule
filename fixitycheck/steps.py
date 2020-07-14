import boto3
import json
import os
import sharedutils

def lambda_handler(event, context):

    state_machine_arn = os.getenv('StateMachineArn')

    for record in event['Records']:

        task_json = record['body']
        taskResponse = "State machine: " + \
            str(sharedutils.execute_step_functions(state_machine_arn, task_json))        
        print(taskResponse)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": taskResponse,
        }),
    }