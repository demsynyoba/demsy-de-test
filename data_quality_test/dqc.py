import os
import sys

root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_path)

from data_quality_test.data_quality.DataQuality import DataQuality


def create_df_from_dq_results(spark, dq_results):
    dq_data = []
    for result in dq_results["results"]:
        if result["success"] == True:
            status = "PASSED"
        else:
            status = "FAILED"
        dq_data.append(
            (
                result["expectation_config"]["kwargs"]["column"],
                result["expectation_config"]["meta"]["metric"],
                status,
                result["expectation_config"]["expectation_type"],
                result["result"]["unexpected_count"],
                result["result"]["element_count"],
                result["result"]["unexpected_percent"],
                float(100 - result["result"]["unexpected_percent"]),
            )
        )
    dq_columns = [
        "column",
        "metric",
        "status",
        "expectation_type",
        "unexpected_count",
        "element_count",
        "unexpected_percent",
        "percent",
    ]
    dq_df = spark.createDataFrame(data=dq_data, schema=dq_columns)
    return dq_df


def dq_check(spark, df, table_id):
    dq = DataQuality(df, f"{root_path}/data_quality_test/config/{table_id}.json")
    dq_results = dq.run_test()
    dq_df = create_df_from_dq_results(spark, dq_results)

    return dq_df, dq_results
