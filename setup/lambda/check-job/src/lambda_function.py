# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import os
import logging
import datetime as dt
import boto3
import json

glue_client = boto3.client('glue')
ssm_client = boto3.client('ssm')

def lambda_handler(event, context):
    try:
        logging.info('Fetching event data from previous step')
        job_details = event['body']['job']['jobDetails']
        logging.info('Checking Job Status')
        response = check_job_status(job_details)
    except Exception as e:
        logging.error("Fatal error", exc_info=True)
        raise e
    return response

def check_job_status(job_details):
    # This function checks the status of the currently running job
    job_response = glue_client.get_job_run(
        JobName=job_details['jobName'], RunId=job_details['jobRunId'])
    json_data = json.loads(json.dumps(
        job_response, default=datetimeconverter))

    job_details['jobStatus'] = json_data.get('JobRun').get('JobRunState')
    response = {
        'jobDetails': job_details
    }

    return response

def datetimeconverter(o):
    if isinstance(o, dt.datetime):
        return o.__str__()
