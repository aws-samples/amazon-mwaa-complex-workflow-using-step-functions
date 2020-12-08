# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import os
import logging
import datetime as dt
import boto3
import json

def lambda_handler(event, context):
    try:
        if isinstance(event, str):
            event = json.loads(event)
        logging.info('Execution Failed. Logging the event for retry. Add your logic here : Event : '+ event)
         except Exception as e:
        logging.error("Fatal error", exc_info=True)
        raise e
    return
