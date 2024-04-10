from lib.utils import (
    schema_reader,
    update_schema_registry,
    append_data_to_destination,
    get_df,
)

TABLE_NAME = "ride_request"
DATASET_NAME = "raw_ride"
SOURCE_FILE_NAME = "ride-request.csv"
EXISTING_FILE_NAME = "ride-request-existing.csv"

if __name__ == "__main__":
    df = get_df(SOURCE_FILE_NAME)
    existing_df = get_df(EXISTING_FILE_NAME)
    new_schema = schema_reader(df)

    updated_schema = update_schema_registry(DATASET_NAME, TABLE_NAME, new_schema)
    append_data_to_destination(DATASET_NAME, TABLE_NAME, df, existing_df)
