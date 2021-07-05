# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

def mask(record):
    record['content'] = '*' * len(record['content'])
    return record

# Resolve the arguments which we speicfied.
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'CONNECTION_NAME',
    'DATABASE_NAME',
    'TABLE_NAME',
    'OUTPUT_BUCKET',
    'OUTPUT_PATH',
])
sparkContext = SparkContext()
glueContext = GlueContext(sparkContext)

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Get the connection information to the Aurora cluster by Glue Connection.
jdbc_conf = glueContext.extract_jdbc_conf(connection_name=args['CONNECTION_NAME'])

connection_options = {
    'url': '{0}/{1}'.format(jdbc_conf['url'], args['DATABASE_NAME']),
    'user': jdbc_conf['user'],
    'password': jdbc_conf['password'],
    'dbtable': args['TABLE_NAME'],
    'hashfield': 'id',
}

# Read the data from Aurora cluster
dynamicframe = glueContext.create_dynamic_frame.from_options(
    connection_type=jdbc_conf['vendor'],
    connection_options=connection_options,
    # If you changed the string value below, the bookmark would be reset.
    transformation_ctx='bookmark',
)

# Apply the mark
dynamicframe = Map.apply(frame=dynamicframe, f=mask)

# Write to the S3 bucket
glueContext.write_dynamic_frame.from_options(
    frame=dynamicframe,
    connection_type='s3',
    connection_options={
        'path': 's3://{0}/{1}'.format(args['OUTPUT_BUCKET'], args['OUTPUT_PATH'])
    },
    format='parquet',
)

job.commit()
