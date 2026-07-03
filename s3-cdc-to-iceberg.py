import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    row_number,
    to_timestamp
)
from pyspark.sql.window import Window

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------

CDC_PATH = "s3://ss-aws-glue-poc/cdc/"
TABLE_NAME = "glue_catalog.fredhopper.product"

# ------------------------------------------------------------------------------
# Initialize Glue
# ------------------------------------------------------------------------------

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger.info(f"Glue Job : {args['JOB_NAME']}")
logger.info(f"Spark Application : {spark.sparkContext.applicationId}")

# ------------------------------------------------------------------------------
# Read CDC Files (Glue Bookmark Enabled)
# ------------------------------------------------------------------------------

dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [CDC_PATH],
        "recurse": True
    },
    format="json",
    transformation_ctx="cdc_source"
)

record_count = dynamic_frame.count()

if record_count == 0:
    logger.info("No new CDC files found.")
    job.commit()
    # sys.exit(0)
else:
    
    df = dynamic_frame.toDF().cache()
    
    logger.info(f"Records Read : {record_count}")
    
    # ------------------------------------------------------------------------------
    # Files Processed
    # ------------------------------------------------------------------------------
    
    logger.info("===== Files Processed =====")
    
    for file in sorted(df.inputFiles()):
        logger.info(file)
    
    # ------------------------------------------------------------------------------
    # CDC Records
    # ------------------------------------------------------------------------------
    
    logger.info("===== CDC Records =====")
    
    df.show(100, truncate=False)
    
    # ------------------------------------------------------------------------------
    # Validate Schema
    # ------------------------------------------------------------------------------
    
    required_columns = [
        "product_id",
        "sku",
        "price",
        "stock",
        "updated_at",
        "cdc_timestamp",
        "op"
    ]
    
    missing = [c for c in required_columns if c not in df.columns]
    
    if missing:
        raise Exception(f"Missing required columns : {missing}")
    
    # ------------------------------------------------------------------------------
    # Validate CDC Operation
    # ------------------------------------------------------------------------------
    
    invalid = df.filter(~col("op").isin("I", "U", "D"))
    
    if invalid.limit(1).count() > 0:
        logger.error("Invalid CDC operation found.")
        invalid.show(truncate=False)
        raise Exception("Invalid CDC operation found.")
    
    # ------------------------------------------------------------------------------
    # Normalize Data
    # ------------------------------------------------------------------------------
    
    df = (
        df
        .withColumn("updated_at", to_timestamp("updated_at"))
        .withColumn("cdc_timestamp", to_timestamp("cdc_timestamp"))
    )
    
    # ------------------------------------------------------------------------------
    # Keep latest record per Product
    # ------------------------------------------------------------------------------
    
    window = (
        Window
        .partitionBy("product_id")
        .orderBy(col("cdc_timestamp").desc())
    )
    
    df = (
        df
        .withColumn("rn", row_number().over(window))
        .filter(col("rn") == 1)
        .drop("rn")
    )
    
    dedup_count = df.count()
    
    logger.info(f"Records After Dedup : {dedup_count}")
    
    # ------------------------------------------------------------------------------
    # Source / Target Schema
    # ------------------------------------------------------------------------------
    
    logger.info("===== Source Schema =====")
    df.printSchema()
    
    logger.info("===== Target Schema =====")
    spark.table(TABLE_NAME).printSchema()
    
    # ------------------------------------------------------------------------------
    # Create Temp View
    # ------------------------------------------------------------------------------
    
    df.createOrReplaceTempView("cdc_stage")
    
    # ------------------------------------------------------------------------------
    # Iceberg MERGE
    # ------------------------------------------------------------------------------
    
    merge_sql = f"""
    MERGE INTO {TABLE_NAME} t
    USING cdc_stage s
    ON t.product_id = s.product_id
    
    WHEN MATCHED
    AND s.op = 'D'
    THEN DELETE
    
    WHEN MATCHED
    AND s.op = 'U'
    AND (
        t.cdc_timestamp IS NULL
        OR s.cdc_timestamp > t.cdc_timestamp
    )
    THEN UPDATE SET
        sku = s.sku,
        price = s.price,
        stock = s.stock,
        updated_at = s.updated_at,
        cdc_timestamp = s.cdc_timestamp
    
    WHEN NOT MATCHED
    AND s.op = 'I'
    THEN INSERT (
        product_id,
        sku,
        price,
        stock,
        updated_at,
        cdc_timestamp
    )
    VALUES (
        s.product_id,
        s.sku,
        s.price,
        s.stock,
        s.updated_at,
        s.cdc_timestamp
    )
    """
    
    try:
    
        logger.info("Executing Iceberg MERGE...")
    
        spark.sql(merge_sql)
    
        logger.info("MERGE completed successfully.")
    
        # --------------------------------------------------------------------------
        # Show Updated Records
        # --------------------------------------------------------------------------
    
        ids = [
            row.product_id
            for row in df.select("product_id").distinct().collect()
        ]
    
        if ids:
    
            values = ",".join(f"'{x}'" for x in ids)
    
            logger.info("===== Updated Records =====")
    
            spark.sql(f"""
            SELECT *
            FROM {TABLE_NAME}
            WHERE product_id IN ({values})
            ORDER BY product_id
            """).show(truncate=False)
    
        # --------------------------------------------------------------------------
        # Commit Bookmark
        # --------------------------------------------------------------------------
    
        job.commit()
    
        logger.info("Glue Bookmark committed successfully.")
        logger.info("CDC batch processed successfully.")
    
    except Exception as e:
    
        logger.error(f"MERGE Failed : {str(e)}")
        raise
    
    finally:
    
        df.unpersist()
