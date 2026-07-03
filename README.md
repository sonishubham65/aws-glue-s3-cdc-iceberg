# Run the Job
aws glue start-job-run \
    --job-name s3-cdc-to-iceberg \
    --arguments '{
        "--conf":"spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.glue_catalog.warehouse=s3://ss-aws-glue-poc/catalogs/ --conf spark.sql.catalog.glue_catalog.glue.skip-name-validation=true",
        "--datalake-formats":"iceberg"
    }'

# Setup Glue Workflow
