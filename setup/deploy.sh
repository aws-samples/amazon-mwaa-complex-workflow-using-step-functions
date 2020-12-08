#!/bin/bash
sflag=false
dflag=false
nflag=false
pflag=false

DIRNAME=$(dirname "$0")

usage () { echo "
    -h -- Opens up this help message
    -n -- Name of the CloudFormation stack
    -p -- Name of the AWS profile to use
    -s -- Name of MWAA S3 bucket to upload artifacts to. Ex- DAGs
    -d -- Name of Demo S3 bucket for data. Ex- Raw data /CFN sripts
"; }
options=':n:p:s:d:h'
while getopts $options option
do
    case "$option" in
        n  ) nflag=true; STACK_NAME=$OPTARG;;
        p  ) pflag=true; PROFILE=$OPTARG;;
        s  ) sflag=true; MWAA_BUCKET=$OPTARG;;
        d  ) dflag=true; S3_BUCKET=$OPTARG;;
        h  ) usage; exit;;
        \? ) echo "Unknown option: -$OPTARG" >&2; exit 1;;
        :  ) echo "Missing option argument for -$OPTARG" >&2; exit 1;;
        *  ) echo "Unimplemented option: -$OPTARG" >&2; exit 1;;
    esac
done

if ! $pflag
then
    echo "-p not specified, using default..." >&2
    PROFILE="default"
fi

if ! $dflag
then
    echo "-d not specified, Please specify the Demo S3Bucket name.."
    exit 1
fi
echo "Checking if bucket exists ..."
if ! aws s3 ls $S3_BUCKET --profile $PROFILE; then
  echo "S3 bucket named $S3_BUCKET does not exist. Create? [Y/N]"
  read choice
  if [ $choice == "Y" ] || [ $choice == "y" ]; then
    echo $PROFILE
    aws s3 mb s3://$S3_BUCKET --profile $PROFILE

  else
    echo "Bucket does not exist. Deploy aborted."
    exit 1
  fi
fi

#Adding the DemoBucket to SSM Parameter
aws ssm put-parameter \
--name "/mwaa/S3/DemoBucket" \
--type "String" \
--value $S3_BUCKET \
--overwrite
if ! $sflag
then
    echo "-s not specified, Please specify the MWAA DAG Bucket name.."
    exit 1
fi



if ! $nflag
then
    STACK_NAME="mwaa-demo-foundations"
fi

echo "Checking if bucket exists ..."
if ! aws s3 ls $MWAA_BUCKET --profile $PROFILE; then
  echo "S3 bucket named $MWAA_BUCKET does not exist. Please verify and try with MWAA DAG bucket name !"
  exit 1
fi

#Storing the MWAA bucket name
aws ssm put-parameter \
--name "/mwaa/S3/mwaaBucket" \
--type "String" \
--value $MWAA_BUCKET \
--overwrite

function send_scripts()
{
  #Copy Glue job script to s3
  aws s3 cp ./movielens_glue_transform.py s3://$S3_BUCKET/scripts/glue_jobs/movielens/
  mkdir $DIRNAME/tmp
  cd $DIRNAME/tmp
  cp -R ../transform $DIRNAME/
  aws s3 cp ./transform/ s3://$S3_BUCKET/scripts/ --recursive
  cd ..
  rm -rf tmp
  #copying MWAA Dag and libraries to MWAA S3 bucket
  aws s3 cp ../dags/ s3://$MWAA_BUCKET/dags/ --recursive
}
mkdir $DIRNAME/output
aws cloudformation package --profile $PROFILE --template-file $DIRNAME/template.yaml --s3-bucket $S3_BUCKET --output-template-file $DIRNAME/output/packaged-template.yaml

echo "Checking if stack exists ... The check will return ValidationError if stack does not exist."
if ! aws cloudformation describe-stacks --profile $PROFILE --stack-name $STACK_NAME; then
  echo -e "Stack does not exist, creating ..."
  aws cloudformation create-stack \
    --stack-name $STACK_NAME \
    --template-body file://$DIRNAME/output/packaged-template.yaml \
    --capabilities "CAPABILITY_NAMED_IAM" "CAPABILITY_AUTO_EXPAND" \
    --profile $PROFILE


  echo "Waiting for stack to be created ..."
  aws cloudformation wait stack-create-complete --profile $PROFILE \
    --stack-name $STACK_NAME
  send_scripts;

else
  echo -e "Stack exists, attempting update ..."

  set +e
  update_output=$( aws cloudformation update-stack \
    --profile $PROFILE \
    --stack-name $STACK_NAME \
    --template-body file://$DIRNAME/output/packaged-template.yaml \
    --capabilities "CAPABILITY_NAMED_IAM" "CAPABILITY_AUTO_EXPAND" 2>&1)
  status=$?
  set -e

  echo "$update_output"

  if [ $status -ne 0 ] ; then
    # Don't fail for no-op update
    if [[ $update_output == *"ValidationError"* && $update_output == *"No updates"* ]] ; then
      echo -e "\nFinished create/update - no updates to be performed";
      send_scripts;
      exit 0;
    else
      exit $status
    fi
  fi

  echo "Waiting for stack update to complete ..."
  aws cloudformation wait stack-update-complete --profile $PROFILE \
    --stack-name $STACK_NAME 
  echo "Finished create/update successfully!"
  send_scripts;
fi