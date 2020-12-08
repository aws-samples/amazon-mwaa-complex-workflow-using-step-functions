# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import boto3, json, pprint, requests, textwrap, time, logging, requests
import os
from datetime import datetime
from typing import Optional, Union


sfn_non_terminal_states = {"RUNNING"}
sfn_failed_states = {"FAILED", "TIMED_OUT", "ABORTED"}

def detect_running_region():
    """Dynamically determine the region from a running MWAA Setup ."""
    easy_checks = [
        # check if set through ENV vars
        os.environ.get('AWS_REGION'),
        os.environ.get('AWS_DEFAULT_REGION'),
        # else check if set in config or in boto already
        boto3.DEFAULT_SESSION.region_name if boto3.DEFAULT_SESSION else None,
        boto3.Session().region_name,
    ]
    for region in easy_checks:
        if region:
            return region
    # else Assuming Airflow is running in an EC2 environment
    # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-identity-documents.html
    r = requests.get("http://169.254.169.254/latest/dynamic/instance-identity/document")
    response_json = r.json()
    return response_json.get('region')

def get_region():
    return 'us-west-2'

def client(region_name):
    global emr
    emr = boto3.client('emr', region_name=region_name)
    global sfn
    sfn = boto3.client('stepfunctions', region_name=region_name)
    global ssm
    ssm = boto3.client('ssm', region_name=region_name)


def create_cluster(region_name, cluster_name='mwaa-emr-' + str(datetime.now()), release_label='emr-5.31.0',master_instance_type='m5.xlarge', num_core_nodes=2, core_node_instance_type='m5.xlarge'):
    cluster_response = emr.run_job_flow(
        Name=cluster_name,
        ReleaseLabel=release_label,
        Instances={
            'InstanceGroups': [
                {
                    'Name': "Master nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': master_instance_type,
                    'InstanceCount': 1
                },
                {
                    'Name': "Slave nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': core_node_instance_type,
                    'InstanceCount': num_core_nodes
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': True
        },
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        Applications=[
            { 'Name': 'hadoop' },
            { 'Name': 'spark' }
        ]
    )
    return cluster_response['JobFlowId']


def wait_for_cluster_creation(cluster_id):
    emr.get_waiter('cluster_running').wait(ClusterId=cluster_id)


def terminate_cluster(cluster_id):
    emr.terminate_job_flows(JobFlowIds=[cluster_id])

def get_demo_bucket_name():
    demo_bucket=ssm.get_parameter(Name="/mwaa/S3/DemoBucket")['Parameter']['Value']
    return demo_bucket

def get_stepfunction_arn():
    sfn_arn=ssm.get_parameter(Name="/mwaa/sfn/movielens")['Parameter']['Value']
    return sfn_arn

def start_execution(
    state_machine_arn: str,
    name: Optional[str] = None,
    state_machine_input: Union[dict, str, None] = None,
) -> str:
    execution_args = {'stateMachineArn': state_machine_arn}
    if name is not None:
        execution_args['name'] = name
    if state_machine_input is not None:
        if isinstance(state_machine_input, str):
            execution_args['input'] = state_machine_input
        elif isinstance(state_machine_input, dict):
            execution_args['input'] = json.dumps(state_machine_input)

    logging.info('Executing Step Function State Machine: %s', state_machine_arn)

    response = sfn.start_execution(**execution_args)
    logging.info('Execution arn: %s', response)
    return response.get('executionArn')

def monitor_stepfunction_run(execution_arn):
    sfn.describe_execution(executionArn=execution_arn)
    sec = 0
    running = True
    check_interval = 30

    while running:
        time.sleep(30)
        sec += check_interval

        try:
            response = sfn.describe_execution(executionArn=execution_arn)
            status = response["status"]
            logging.info(
                "Step function still running for %s seconds... " "current status is %s",
                sec,
                status,
            )
        except KeyError:
            raise AirflowException("Could not get status of the Step function workflow")
        except ClientError:
            raise AirflowException("AWS request failed, check logs for more info")

        if status in sfn_non_terminal_states:
            running = True
        elif status in sfn_failed_states:
            raise AirflowException(
                "Step function failed.Check Step functions log for details"
            )
        else:
            running = False
    logging.info("Step function workflow completed")
    response = sfn.describe_execution(executionArn=execution_arn)
    return response.get('executionArn')