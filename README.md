# AWS Glue ETL Sample CDK

This project deploys a minimum ETL workload using AWS Glue. It loads data from Aurora cluster and store the ETL results to S3 bucket as parquet format.
The Glue job is quite simple that replaces "content" column of the table to "*". (content: `Hello` => `*****`)

## Deployment

You need to setup your CDK environment. See [Getting started with the AWS CDK](https://docs.aws.amazon.com/cdk/latest/guide/getting_started.html).

```bash
cdk deploy
```

## Testing

First, you need to create demo data in Aurora cluster. We deployed a lambda function that inserts 1000 records to the database. Let's invoke it by below.

```bash
aws lambda invoke --function-name create-demo-data /dev/null
```

Next, run the Glue job to do the ETL. Go to [AWS Glue Console (Jobs)](https://console.aws.amazon.com/glue/home#etl:tab=jobs) and select **AwsGlueEtlSampleCdk**. Then click **Action** and **Run job**.

After the job succeeds, go to [AWS Glue Console (Crawlers)](https://console.aws.amazon.com/glue/home#catalog:tab=crawlers) and select **AwsGlueEtlSampleCdk**. Then click **Run crawler**.

After the crawler succeeds, go to [Athena (Query)](https://console.aws.amazon.com/athena/home#query) and select **AwsDataCatalog** as **Data source** and **mydatabase** as **Database**.
Then enter the following query in the box. Then click **Run query**.

```sql
SELECT * FROM mytable;
```

As you can see, the "content" column is masked by "*".

To get the number of records, run the query below.

```sql
SELECT COUNT(*) FROM mytable;
```

You will get 1000 as the result if you invoked the lambda function once.

## Cleaning

```bash
cdk destroy
```

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
