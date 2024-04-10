from lib.utils import (
    schema_reader,
    update_schema_registry,
    append_data_to_destination,
    get_df,
    data_quality_checker,
)

TABLE_NAME = "taxi_trip"
DATASET_NAME = "raw_ride"
SOURCE_FILE_NAME = "taxi-trip.csv"

if __name__ == "__main__":
    df = get_df(SOURCE_FILE_NAME)
    new_schema = schema_reader(df)

    dqcheck = data_quality_checker(df, DATASET_NAME, TABLE_NAME)

    if dqcheck:
        updated_schema = update_schema_registry(DATASET_NAME, TABLE_NAME, new_schema)
        append_data_to_destination(DATASET_NAME, TABLE_NAME, df)
