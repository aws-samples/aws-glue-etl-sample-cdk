# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import boto3
import os
import random
import string

rdsData = boto3.client('rds-data')

def random_string(n):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=n))

def handler(event, context):
    clusterArn = os.environ['CLUSTER_ARN']
    secretArn = os.environ['SECRET_ARN']
    database = os.environ['DATABASE']
    table = os.environ['TABLE']

    queryCreateDatabase = F"""
CREATE DATABASE IF NOT EXISTS {database}
"""

    queryCreateTable = F"""
CREATE TABLE IF NOT EXISTS \
{table}( \
    id INT NOT NULL AUTO_INCREMENT, \
    content TEXT NOT NULL, \
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP, \
    PRIMARY KEY (id)
)
"""

    queryInsert = F"""
INSERT INTO {table}(content) VALUES(:content)
"""

    response1 = rdsData.execute_statement(
        resourceArn=clusterArn,
        secretArn=secretArn,
        database=database,
        sql=queryCreateDatabase,
    )

    print(response1)

    response2 = rdsData.execute_statement(
        resourceArn=clusterArn,
        secretArn=secretArn,
        database=database,
        sql=queryCreateTable,
    )

    print(response2)

    params = []

    for i in range(1000):
        params.append([{ 'name': 'content', 'value': { 'stringValue': random_string(16) } }])

    response3 = rdsData.batch_execute_statement(
        resourceArn=clusterArn,
        secretArn=secretArn,
        database=database,
        sql=queryInsert,
        parameterSets=params,
    )

    print(response3)
