from enum import Enum
import os
import json
import pandas as pd
from google.cloud import bigquery

# check bigquery connection
client = bigquery.Client(project=os.environ.get("GCP_PROJECT_NAME"))


class WriteType(Enum):
    APPEND = "APPEND"
    WRITE_TRUNCATE = "WRITE_TRUNCATE"


def write_to_bq(df: pd.DataFrame, name: str, dataset_name: str, write_type: WriteType = WriteType.APPEND):  # writes to bigquery, types supported are APPEND or WRITE_TRUNCATE
    project_id = os.environ.get("GCP_PROJECT_NAME")
    client = bigquery.Client(project=project_id)
    dataset = client.dataset(dataset_name)
    table = dataset.table(name)
    if write_type == WriteType.APPEND:
        write_type = bigquery.WriteDisposition.WRITE_APPEND
        existing_df = client.query(f"SELECT * FROM {project_id}.{dataset.dataset_id}.{table.table_id}").to_dataframe()

        if sorted(existing_df.columns.tolist()) != sorted(df.columns.tolist()):
            print("[ERROR] Columns do not match. Cannot append data")
            return
        df = pd.concat([existing_df, df]).reset_index(drop=True).drop_duplicates()
        if len(df) == len(existing_df):
            print("No new data to append")
            return
        elif df.shape[1] != existing_df.shape[1]:
            print("[ERROR] Columns do not match. Cannot append data")
            return
    elif write_type == WriteType.WRITE_TRUNCATE:
        write_type = bigquery.WriteDisposition.WRITE_TRUNCATE

    job_config = bigquery.LoadJobConfig(write_disposition=write_type)
    try:
        json_data = df.to_json(orient="records")
        json_object = json.loads(json_data)
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job = client.load_table_from_json(json_object, table, job_config=job_config)
        job.result()
    except:
        job = client.load_table_from_dataframe(df, table, job_config=job_config)
        job.result()
    print("Uploaded", name, "successfully of length", len(df))
    table = client.get_table(table)  # Make an API request.
    print("New Length of {} rows and {} columns".format(table.num_rows, len(table.schema)))
    client.close()
    return
