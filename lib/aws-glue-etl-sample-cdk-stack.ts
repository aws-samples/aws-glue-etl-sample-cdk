// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
import * as cdk from '@aws-cdk/core';
import * as ec2 from '@aws-cdk/aws-ec2';
import * as rds from '@aws-cdk/aws-rds';
import * as glue from '@aws-cdk/aws-glue';
import * as assets from '@aws-cdk/aws-s3-assets';
import * as s3 from '@aws-cdk/aws-s3';
import * as iam from '@aws-cdk/aws-iam';
import * as lambda from '@aws-cdk/aws-lambda-python';
import * as path from 'path';

export class AwsGlueEtlSampleCdkStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Create VPC
    const vpc = new ec2.Vpc(this, 'VPC', {
      maxAzs: 2,
      subnetConfiguration: [
        {
          name: 'public',
          subnetType: ec2.SubnetType.PUBLIC,
        },
        {
          name: 'private',
          subnetType: ec2.SubnetType.PRIVATE,
        },
      ],
    });

    // Create SecurityGroup for Aurora
    const securityGroupAurora = new ec2.SecurityGroup(this, 'SecurityGroupAurora', { vpc });
    securityGroupAurora.addIngressRule(ec2.Peer.ipv4(vpc.vpcCidrBlock), ec2.Port.tcp(3306));

    // Target database name
    const databaseName = 'mydatabase';

    // Target table name
    const tableName = 'mytable';

    // Aurora credentials
    const databaseCredentials = rds.Credentials.fromGeneratedSecret('clusteradmin');

    // Create Aurora serverless cluster
    const serverlessCluster = new rds.ServerlessCluster(this, 'ServerlessCluster', {
      engine: rds.DatabaseClusterEngine.auroraMysql({
        // 5.7.mysql_aurora.2.09.2
        version: rds.AuroraMysqlEngineVersion.VER_2_09_2,
      }),
      credentials: databaseCredentials,
      vpc,
      vpcSubnets: {
        subnetType: ec2.SubnetType.PRIVATE,
      },
      securityGroups: [securityGroupAurora],
      enableDataApi: true,
      defaultDatabaseName: databaseName,
      scaling: {
        // Keep the cluster awake
        autoPause: cdk.Duration.seconds(0),
      },
    });

    // Create SecurityGroup for Glue connection
    const securityGroupConnection = new ec2.SecurityGroup(this, 'SecurityGroupConnection', {
      allowAllOutbound: true,
      vpc,
    });

    // Allow inbound traffic from Glue connection to Aurora cluster
    securityGroupAurora.connections.allowFrom(securityGroupConnection.connections, ec2.Port.allTcp());

    // Allow internal traffic internally
    securityGroupConnection.connections.allowInternally(ec2.Port.allTcp());

    // Create Glue connection
    const connectionAurora = new glue.Connection(this, 'Connection', {
      type: glue.ConnectionType.JDBC,
      properties: {
        JDBC_CONNECTION_URL: `jdbc:mysql://${serverlessCluster.clusterEndpoint.socketAddress}/${databaseName}`,
        USERNAME: serverlessCluster.secret!.secretValueFromJson('username').toString(),
        PASSWORD: serverlessCluster.secret!.secretValueFromJson('password').toString(),
      },
      securityGroups: [securityGroupConnection],
      subnet: vpc.privateSubnets[0],
    });

    // Upload job script as S3 asset
    const scriptAsset = new assets.Asset(this, 'Script', {
      path: path.join(__dirname, '..', 'glue', 'job1.py'),
    });

    // Create Glue Job output bucket
    const jobOutputBucket = new s3.Bucket(this, 'JobOutputBucket', {});

    // Create Glue Job role
    const jobRole = new iam.Role(this, 'JobRole', {
      assumedBy: new iam.ServicePrincipal('glue'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'),
      ],
    });

    // Allow job role to read script
    scriptAsset.grantRead(jobRole);

    // Allow job role to read and write to output bucket
    jobOutputBucket.grantReadWrite(jobRole);

    // Create Glue Job
    const job = new glue.CfnJob(this, 'Job', {
      glueVersion: '2.0',
      name: 'AwsGlueEtlSampleCdk',
      role: jobRole.roleArn,
      connections: {
        connections: [connectionAurora.connectionName],
      },
      command: {
        name: 'glueetl',
        pythonVersion: '3',
        scriptLocation: `s3://${scriptAsset.s3BucketName}/${scriptAsset.s3ObjectKey}`,
      },
      defaultArguments: {
        '--job-bookmark-option': 'job-bookmark-enable',
        '--enable-metrics': '',
        '--enable-continuous-cloudwatch-log': 'true',
        '--CONNECTION_NAME': connectionAurora.connectionName,
        '--DATABASE_NAME': databaseName,
        '--TABLE_NAME': tableName,
        '--OUTPUT_BUCKET': jobOutputBucket.bucketName,
        '--OUTPUT_PATH': tableName,
      },
      timeout: 60 * 24,
    });

    // Create Glue Crawler role
    const crawlerRole = new iam.Role(this, 'CrawlerRole', {
      assumedBy: new iam.ServicePrincipal('glue'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'),
      ],
    });

    // Allow crawler role to read job output bucket
    jobOutputBucket.grantRead(crawlerRole);

    // Create Glue Database
    const crawlerOutput = new glue.Database(this, 'CrawlerOutput', {
      databaseName: databaseName,
    });

    // Create Glue Crawler
    const crawler = new glue.CfnCrawler(this, 'Crawler', {
      name: 'AwsGlueEtlSampleCdk',
      role: crawlerRole.roleArn,
      databaseName: crawlerOutput.databaseName,
      targets: {
        s3Targets: [
          {
            path: `s3://${jobOutputBucket.bucketName}/${tableName}`,
          }
        ],
      },
    });

    // Create Lambda function to create demo data
    const createDemoData = new lambda.PythonFunction(this, 'CreateDemoData', {
      functionName: 'create-demo-data',
      entry: path.join(__dirname, '..', 'lambda'),
      index: 'create-demo-data.py',
      environment: {
        CLUSTER_ARN: serverlessCluster.clusterArn,
        SECRET_ARN: serverlessCluster.secret!.secretArn,
        DATABASE: databaseName,
        TABLE: tableName,
      },
      timeout: cdk.Duration.minutes(15),
    });

    // Allow Lambda function to access data api
    serverlessCluster.grantDataApiAccess(createDemoData);
  }
}
