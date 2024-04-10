import os
import sys

root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_path)

from unittest import mock
from unittest.mock import MagicMock, patch

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType
)


from src.lib.utils import (
    get_df,
    session_terminator,
    schema_reader,
    compare_schema,
    update_schema_registry,
    append_data_to_destination
)

def test_session_terminator():
    spark_session_mock = MagicMock()

    session_terminator(spark_session_mock)
    spark_session_mock.stop.assert_called_once()


def test_get_df(mocker, tmp_path):
    spark_session = (
        SparkSession.builder.appName("SchemaEnforcement")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.streaming.stopGracefullyOnShutdown", True)
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")
        .config("spark.driver.host", "localhost")
        .getOrCreate()
    )

    data_dict = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
    ]
    expected_df = spark_session.createDataFrame(data_dict).select(
        col("id").cast('int'),
        col("name")
    )

    with patch("src.lib.utils.root_path", tmp_path) as mock_path:
        temp_dir = tmp_path / "data-source"
        temp_dir.mkdir()

        file_name = "example.csv"
        temp_file = temp_dir / file_name
        temp_file.write_text("id,name\n1,Alice\n2,Bob\n")

        actual_df = get_df(file_name)

        assert sorted(expected_df.collect()) == sorted(actual_df.collect())

    spark_session.stop()


def test_schema_reader(capfd):
    # Mocking the DataFrame
    df_mock = MagicMock()

    schema = schema_reader(df_mock)

    df_mock.printSchema.assert_called_once()
    df_mock.schema.jsonValue.assert_called_once()
    assert schema == df_mock.schema.jsonValue()


def test_compare_schema():
    registered_schema = {
        "fields": [
            {"name": "id", "type": "integer"},
            {"name": "name", "type": "string"},
            {"name": "age", "type": "integer"},
            {"name": "city", "type": "string"},
        ]
    }

    source_schema = {
        "fields": [
            {"name": "id", "type": "integer"},
            {"name": "name", "type": "string"},
            {"name": "age", "type": "string"},
            {"name": "dob", "type": "string"},
        ]
    }

    # Call the function with the mocked schemas
    diffs = compare_schema(registered_schema, source_schema)

    # Assert returned differences
    assert diffs == {
        "new_column": {"dob"},
        "deleted_column": {"city"},
        "diff_data_type": [
            {"name": "age", "registered_type": "integer", "source_type": "string"}
        ],
    }


def test_update_schema_registry_new_file():
    dataset = "test_dataset"
    table_name = "test_table"

    source_schema = {
        "fields": [
            {"name": "id", "type": "integer"},
            {"name": "name", "type": "string"},
            {"name": "age", "type": "string"},
            {"name": "dob", "type": "string"},
        ]
    }

    with patch("pathlib.Path.exists", return_value=False):
        with patch("builtins.open", mock.mock_open()) as new_open:
            with patch("json.dump", return_value=MagicMock()) as mock_dump:

                update_schema_registry(dataset, table_name, source_schema)

                mock_dump.assert_called_once_with(
                    source_schema, new_open(), ensure_ascii=False, indent=4
                )


def test_update_schema_registry_update_file():
    dataset = "test_dataset"
    table_name = "test_table"

    source_schema = {
        "fields": [
            {"name": "id", "type": "integer"},
            {"name": "name", "type": "string"},
            {"name": "age", "type": "string"},
            {"name": "dob", "type": "string"},
        ]
    }

    registered_schema_mock = MagicMock(
        return_value={
            "fields": [
                {"name": "id", "type": "integer"},
                {"name": "name", "type": "string"},
                {"name": "age", "type": "integer"},
                {"name": "city", "type": "string"},
            ]
        }
    )

    expected_schema = {
        "fields": [
            {"name": "id", "type": "integer"},
            {"name": "name", "type": "string"},
            {"name": "age", "type": "string"},
            {"name": "city", "type": "string"},
            {"name": "dob", "type": "string"},
        ]
    }

    compare_schema_return = {
            "new_column": {"dob"},
            "deleted_column": {"city"},
            "diff_data_type": [
                {"name": "age", "registered_type": "integer", "source_type": "string"}
            ],
        }

    with patch("pathlib.Path.exists", return_value=True):
        with patch("src.lib.utils.compare_schema", return_value=compare_schema_return) as compare_mock:
            with patch("json.load", registered_schema_mock):
                with patch("builtins.open", mock.mock_open()) as new_open:
                    with patch("json.dump", return_value=MagicMock()) as mock_dump:

                        updated_schema = update_schema_registry(
                            dataset, table_name, source_schema
                        )

                        compare_mock.assert_called_once()
                        mock_dump.assert_called_once_with(
                            updated_schema, new_open(), ensure_ascii=False, indent=4
                        )
                        assert updated_schema == expected_schema


def test_append_data_to_destination_table_exist():
    dataset = "test_dataset"
    table_name = "test_table"

    spark_source_df = MagicMock()
    spark_existing_df = MagicMock()

    registered_schema_mock = MagicMock(
        return_value={
                "fields": [
                {"name": "id", "type": "integer", "nullable": True},
                {"name": "name", "type": "string", "nullable": True},
                {"name": "age", "type": "integer", "nullable": True},
                {"name": "city", "type": "string", "nullable": True},
                {"name": "dob", "type": "string", "nullable": True},
            ]
        }
    )

    expected_schema_struct = StructType([StructField('id', IntegerType(), True), StructField('name', StringType(), True), StructField('age', IntegerType(), True), StructField('city', StringType(), True), StructField('dob', StringType(), True)])

    with patch("builtins.open", mock.mock_open()) as new_open:
        with patch("json.load", registered_schema_mock):
            with patch("pyspark.sql.SparkSession.createDataFrame", return_value=MagicMock()) as mock_create_df:
                with patch("src.lib.utils.session_terminator") as terminator_mock:
                    updated_df = append_data_to_destination(dataset, table_name, spark_source_df, spark_existing_df)

                    spark_existing_df.unionByName.assert_called_once_with(
                        spark_source_df, allowMissingColumns=True
                    )

                    mock_create_df.assert_called_once_with(spark_existing_df.unionByName.return_value.rdd, expected_schema_struct)
                    updated_df.repartition.assert_called_once_with(1)

                    terminator_mock.assert_called_once()
                    
