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
        crawler_name1 = "mwaa-movielens-curated-crawler-popular-movies"
        crawler_name2 = "mwaa-movielens-curated-crawler-toprated-movies"
        logging.info('Starting Crawler {}'.format(crawler_name1))
        logging.info('Starting Crawler {}'.format(crawler_name2))
        try:
            glue_client.start_crawler(Name=crawler_name1)
            glue_client.start_crawler(Name=crawler_name2)
        except client.exceptions.CrawlerRunningException:
            logging.info('Crawler is already running')
    except Exception as e:
        logging.error("Fatal error", exc_info=True)
        raise e
    return 200
