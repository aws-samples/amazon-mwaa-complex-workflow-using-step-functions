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
        # Submitting a new Glue Job
        job_name="mwaa-movielens-glue-job"
        job_response = glue_client.start_job_run(
            JobName=job_name,
            Arguments={
                # Specify any arguments needed based on bucket and keys (e.g. input/output S3 locations)
                '--JOB_NAME': job_name,
                '--SOURCE_LOCATION': '{}'.format(get_demo_bucket_name()),
                '--job-bookmark-option': 'job-bookmark-enable'
            },
            MaxCapacity=2.0
        )
        # Collecting details about Glue Job after submission (e.g. jobRunId for Glue)
        json_data = json.loads(json.dumps(
            job_response, default=datetimeconverter))
        job_details = {
            "jobName": job_name,
            "jobRunId": json_data.get('JobRunId'),
            "jobStatus": 'STARTED'
        }
        response = {
            'jobDetails': job_details
        }
    except Exception as e:
        logging.error("Fatal error", exc_info=True)
        raise e
    return response

def get_demo_bucket_name():
    demo_bucket=ssm_client.get_parameter(Name="/mwaa/S3/DemoBucket")['Parameter']['Value']
    return demo_bucket

def datetimeconverter(o):
    if isinstance(o, dt.datetime):
        return o.__str__()
