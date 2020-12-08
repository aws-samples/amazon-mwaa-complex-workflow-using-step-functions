# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import json
from pyspark.sql import Row
from pyspark.sql.functions import *

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','SOURCE_LOCATION'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

source_location = args['SOURCE_LOCATION']

movies = spark.read.parquet(f"s3://{source_location}/output/movie/")
ratings = spark.read.parquet(f"s3://{source_location}/output/ratings/")
#Most Popular movies
most_popular = ratings.groupBy("movieId").agg(count("userId")).withColumnRenamed("count(userId)", "num_ratings").sort(desc("num_ratings"))
most_popular_movies = most_popular.join(movies, "movieId")

#Top rated movies
top_rated = ratings.groupBy("movieId").agg(avg(col("rating"))).withColumnRenamed("avg(rating)", "avg_rating").sort(desc("avg_rating"))
top_rated_movies = top_rated.join(movies, "movieId")

most_popular_movies.write.mode('overwrite').parquet(f"s3://{source_location}/curated/most_popular_movies/")
top_rated_movies.write.mode('overwrite').parquet(f"s3://{source_location}/curated/top_rated_movies/")

job.commit()
