import os
import sys

root_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(root_path)

import logging
import json
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
)

from data_quality_test.dqc import dq_check

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)

spark_session = (
    SparkSession.builder.appName("SchemaEnforcement")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.streaming.stopGracefullyOnShutdown", True)
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")
    .config("spark.driver.host", "localhost")
    .getOrCreate()
)


def session_terminator(spark_session):
    spark_session.stop()


def get_df(file_name):
    df = (
        spark_session.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{root_path}/data-source/{file_name}")
    )
    df.show()

    return df


def schema_reader(df):
    schema = df.schema.jsonValue()
    df.printSchema()

    return schema


def compare_schema(registered_schema, source_schema):
    registered_schema_columns = set(
        [field.get("name") for field in registered_schema.get("fields")]
    )
    source_schema_columns = set(
        [field.get("name") for field in source_schema.get("fields")]
    )

    # Get columns diff
    new_column = source_schema_columns - registered_schema_columns
    deleted_column = registered_schema_columns - source_schema_columns

    # Get different data type(s)
    different_data_types = []
    for column_source, column_registry in zip(
        source_schema.get("fields"), registered_schema.get("fields")
    ):
        if column_source.get("name") in registered_schema_columns.intersection(
            source_schema_columns
        ) and column_source.get("type") != column_registry.get("type"):
            schema_diff = {}
            schema_diff["name"] = column_source.get("name")
            schema_diff["registered_type"] = column_registry.get("type")
            schema_diff["source_type"] = column_source.get("type")

            different_data_types.append(schema_diff)

    logging.info("NEW COLUMN(S):")
    logging.info(new_column)

    logging.info("DELETED COLUMN(S):")
    logging.info(deleted_column)

    logging.info("COLUMN(S) WITH DIFFERENT DATA TYPE:")
    logging.info(different_data_types)

    return {
        "new_column": new_column,
        "deleted_column": deleted_column,
        "diff_data_type": different_data_types,
    }


def update_schema_registry(dataset, table_name, schema):
    # ASSUMPTION:
    #   - Schema registry should be stored in bucket or table
    #   - But for now, I'll put it in a file instead

    sr_path = f"{root_path}/schema-registry/{dataset}_{table_name}.json"
    sr_file = Path(sr_path)

    if not sr_file.exists():
        with open(sr_path, "w", encoding="utf-8") as f:
            json.dump(schema, f, ensure_ascii=False, indent=4)

    else:
        with open(sr_path) as f:
            registered_schema = json.load(f)

        schema_update = compare_schema(registered_schema, schema)

        # Add new column(s) to schema registry
        for sch in schema_update.get("new_column"):
            temp_new_col = [
                col for col in schema.get("fields") if sch == col.get("name")
            ]
            registered_schema.get("fields").append(temp_new_col[0])

        # Change data type in schema registry
        for dtype in schema_update.get("diff_data_type"):
            for reg_schema in registered_schema.get("fields"):
                if reg_schema.get("name") == dtype.get("name"):
                    reg_schema["type"] = dtype.get("source_type")

        logging.info("NEW SCHEMA: ")
        logging.info(json.dumps(registered_schema, indent=4))

        # Update schema registry file
        with open(sr_path, "w", encoding="utf-8") as f:
            json.dump(registered_schema, f, ensure_ascii=False, indent=4)

        return registered_schema


def append_data_to_destination(dataset, table_name, source_df, existing_df=None):
    # ASSUMPTION:
    #   - Since I don't use any DB table, I put existing data in data-source/......-existing.csv
    #   - This function should append/insert new data to existing table using the updated schema
    #   - But, because there's no real table involved, I'll just append the existing data with
    #     incoming data, using the new schema, and put it in an output file inside "out" folder

    # Convert JSON schema to Struct
    sr_path = f"{root_path}/schema-registry/{dataset}_{table_name}.json"
    with open(sr_path) as f:
        registered_schema_json = json.load(f)

    schema_fields = []
    for field in registered_schema_json.get("fields"):
        field_name = field["name"]
        field_type = field["type"]
        nullable = field["nullable"]
        if field_type == "integer":
            schema_fields.append(StructField(field_name, IntegerType(), nullable))
        elif field_type == "string":
            schema_fields.append(StructField(field_name, StringType(), nullable))
        elif field_type == "double":
            schema_fields.append(StructField(field_name, DoubleType(), nullable))
        else:
            schema_fields.append(StructField(field_name, StringType(), nullable))

    registered_schema = StructType(schema_fields)

    if existing_df:
        # Merge source and existing data
        merged_df = existing_df.unionByName(source_df, allowMissingColumns=True)
    else:
        merged_df = source_df

    # Update data type by using new schema
    updated_df = spark_session.createDataFrame(merged_df.rdd, registered_schema)
    print(type(updated_df))
    updated_df.show()
    updated_df.printSchema()

    updated_df.repartition(1).write.format("com.databricks.spark.csv").mode(
        "overwrite"
    ).option("header", "true").save(f"{root_path}/out/{dataset}_{table_name}")

    session_terminator(spark_session)

    return updated_df


def data_quality_checker(df, dataset, table):
    table_id = f"{dataset}_{table}"
    dq_df, dq_result = dq_check(spark_session, df, table_id)
    dq_df.show(dq_df.count(), False)

    if dq_result.get("success"):
        return True
    else:
        logging.info(
            "Data Quality Check: FAILED. Skipping data insertion to destination ..."
        )
        return False
