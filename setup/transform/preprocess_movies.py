# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#!/usr/bin/python
# -*- coding: utf-8 -*-

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark import SQLContext
from itertools import islice
from pyspark.sql.functions import col
import os
import sys

if __name__ == '__main__':


    bucket=sys.argv[1]
    spark = SparkSession.builder.appName('Pre Process Movie Dataset').getOrCreate()
    movies = spark.read.option("header", "true").csv(f"s3://{bucket}/raw/movies.csv")
    movies.write.mode('overwrite').parquet(f"s3://{bucket}/output/movie/")