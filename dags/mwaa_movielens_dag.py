# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import mwaalib.workflow_lib as etlclient
import os
import shlex

from airflow import DAG
from airflow.utils import dates
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from mwaalib.emr_submit_and_monitor_step import EmrSubmitAndMonitorStepOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dates.days_ago(0),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True
}

# Initialize the DAG
# Concurrency --> Number of tasks allowed to run concurrently
dag = DAG('mwaa_movie_lens_demo', concurrency=3, schedule_interval=None, default_args=default_args)
region = etlclient.detect_running_region()
etlclient.client(region_name=region)


# Creates an EMR cluster
def create_emr(**kwargs):
    cluster_id = etlclient.create_cluster(region_name=region, cluster_name='mwaa_emr_cluster', num_core_nodes=2)
    return cluster_id


# Waits for the EMR cluster to be ready to accept jobs
def wait_for_completion(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    etlclient.wait_for_cluster_creation(cluster_id)


# Terminates the EMR cluster
def terminate_emr(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    etlclient.terminate_cluster(cluster_id)

# Monitors Step function execution
def monitor_step_function(**kwargs):
    ti = kwargs['ti']
    execution_arn = ti.xcom_pull(task_ids='trigger_stepfunctions')
    response = etlclient.monitor_stepfunction_run(execution_arn)
    return response

# Starts the Step Function
def trigger_step_function(**kwargs):
    # get the Step function arn
    statemachine_arn = kwargs['stateMachineArn']
    sfn_execution_arn = etlclient.start_execution(statemachine_arn)
    return sfn_execution_arn


# Define the individual tasks using Python Operators
create_cluster = PythonOperator(
    task_id='create_cluster',
    python_callable=create_emr,
    dag=dag)

wait_for_cluster_completion = PythonOperator(
    task_id='wait_for_cluster_completion',
    python_callable=wait_for_completion,
    dag=dag)



extract_load_data = EmrSubmitAndMonitorStepOperator(
    task_id="extract_load_data",
    trigger_rule="one_success",
    steps=[
        {
            "Name": "extract_load_data",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "/bin/bash",
                    "-c",
                    "wget http://files.grouplens.org/datasets/movielens/ml-latest.zip && unzip ml-latest.zip && aws s3 cp ml-latest s3://{}/raw --recursive".format(
                        etlclient.get_demo_bucket_name()),
                ],
            },
        }
    ],
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    check_interval=int("30"),
    job_name="extract_load_data",
    dag=dag
)

preprocess_movies = EmrSubmitAndMonitorStepOperator(
    task_id="preprocess_movies",
    steps=[
        {
            "Name": "preprocess_movies",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--conf",
                    "deploy-mode=cluster",
                    "s3://{}/scripts/preprocess_movies.py".format(etlclient.get_demo_bucket_name()),
                    etlclient.get_demo_bucket_name()
                ],
            },
        }
    ],
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    check_interval=int("30"),
    job_name="preprocess_movies",
    dag=dag
)

preprocess_ratings = EmrSubmitAndMonitorStepOperator(
    task_id="preprocess_ratings",
    steps=[
        {
            "Name": "preprocess_ratings",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--conf",
                    "deploy-mode=cluster",
                    "s3://{}/scripts/preprocess_ratings.py".format(etlclient.get_demo_bucket_name()),
                    etlclient.get_demo_bucket_name()
                ],
            },
        }
    ],
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    check_interval=int("30"),
    job_name="preprocess_ratings",
    dag=dag
)

preprocess_tags = EmrSubmitAndMonitorStepOperator(
    task_id="preprocess_tags",
    steps=[
        {
            "Name": "preprocess_tags",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--conf",
                    "deploy-mode=cluster",
                    "s3://{}/scripts/preprocess_tags.py".format(etlclient.get_demo_bucket_name()),
                    etlclient.get_demo_bucket_name()
                ],
            },
        }
    ],
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    check_interval=int("30"),
    job_name="preprocess_ratings",
    dag=dag
)

terminate_cluster = PythonOperator(
    task_id='terminate_cluster',
    python_callable=terminate_emr,
    trigger_rule='all_done',
    dag=dag)

trigger_sfn = PythonOperator(
    task_id='trigger_stepfunctions',
    python_callable=trigger_step_function,
    op_kwargs={'stateMachineArn': etlclient.get_stepfunction_arn()},
    dag=dag)

monitor_sfn = PythonOperator(
    task_id='monitor_stepfunctions',
    python_callable=monitor_step_function,
    dag=dag)

# construct the DAG by setting the dependencies
create_cluster >> wait_for_cluster_completion
wait_for_cluster_completion >> extract_load_data
extract_load_data >> preprocess_movies >> terminate_cluster
extract_load_data >> preprocess_ratings >> terminate_cluster
extract_load_data >> preprocess_tags >> terminate_cluster
terminate_cluster >> trigger_sfn >> monitor_sfn
