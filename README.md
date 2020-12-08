# Build complex workflows with Amazon MWAA,AWS Step Functions ,AWS Glue and Amazon EMR

Important: this application uses various AWS services and there are costs associated with these services after the Free Tier usage - please see the [AWS Pricing page](https://aws.amazon.com/pricing/) for details. You are responsible for any AWS costs incurred. No warranty is implied in this example.
##Code repo structure

```bash
.
├── README.MD                   <-- The instructions file
├── dags/mwaalib                <-- Reusable code for Amazon EMR and AWS Step Functions
├── setup                       <-- Source code for initial setup
│   └── transform/              <-- Pre processing pyspark code and resuable code.     
│   └── template.yaml           <-- Template for basic application setup
│   └── deploy.sh               <-- Deploy Script 

```

## Requirements

* AWS CLI already configured with Administrator permission

## Prerequisites
1.  AWS Account .[Create an AWS account](https://portal.aws.amazon.com/gp/aws/developer/registration/index.html) if you do not already have one and login.
2.  Amazon Managed Workflow for Apache Airflow environment in supported region.[Create environment](https://us-west-2.console.aws.amazon.com/mwaa/home?region=us-west-2#/create/environment) if you do not have one. Note us-west-2 is selected. Change the region, if required.
3.  IAM permissions for the MWAA Execution role for S3 access

        elasticmapreduce:RunJobFlow
        iam:PassRole on EMR_DEFAULT_ROLE
        iam:PassRole on EMR_EC2_ROLE
        "states:DescribeStateMachineForExecution",
        "states:DescribeStateMachine",
        "states:DescribeExecution",
        "states:StartExecution",
        "elasticmapreduce:*"
 
 A sample policy is provided as an example. [Policy](setup/additional_policy.json)
 A sample role yaml is also provided if you do not have EMR_DEFAULT_ROLE and EMR_EC2_ROLE already created. [EMR Roles](setup/default-emr-roles.yaml)
 

## Installation Instructions

1. [Create an AWS account](https://portal.aws.amazon.com/gp/aws/developer/registration/index.html) if you do not already have one and login.
2. Clone the repo onto your local development machine using `git clone`.
3. From the command line, change directory into the ```setup``` folder, then run:
    ```
    ./deploy.sh -s <MWAA Airflow Dag Bucket Name> -d <Demo Data Bucket Name>
    ./deploy.sh -s airflow-dipankar-us-east-1 -d mwaa-dl-demo-us-east-1
    ```
   Modify the stack-name or bucket parameters as needed. Wait for the stack to complete.


## AWS resources :
Following stacks are created by the above process
1. ```mwaa-demo-foundations``` - Contains the foundational resources and services 
    Glue Database - mwaa-movielens-demo-db
    Glue Crawlers  - Crawlers to catalog the data.
    Lambda Functions - To invoke Glue jobs and check status from Step Functions  
    LambdaRole - Lambda role for Routing Lambda
    SSM Parameters -  SSM parameters for resources to be used by all services.
    Step Functions -  Data Curate Step function


## AWS resources created based on DAG Run:
1. EMR Cluster

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

