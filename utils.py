import os
import json
import pandas as pd
from google.cloud import bigquery
import difflib
import psutil
from enum import Enum


class WriteType(Enum):
    APPEND = "APPEND"
    WRITE_TRUNCATE = "WRITE_TRUNCATE"


class League(Enum):
    EPL = "EPL"
    La_Liga = "La Liga"
    Serie_A = "Serie A"
    Ligue_1 = "Ligue 1"
    Bundesliga = "Bundesliga"
    Eredivisie = "Eredivisie"
    Primeira_Liga = "Primeira Liga"


def write_to_bq(df: pd.DataFrame, name: str, dataset_name: str, write_type="APPEND"):  # writes to bigquery, types supported are APPEND or WRITE_TRUNCATE
    project_id = os.environ.get("GCP_PROJECT_NAME")
    client = bigquery.Client(project=project_id)
    dataset = client.dataset(dataset_name)
    table = dataset.table(name)
    if write_type == "APPEND":
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
    elif write_type == "WRITE_TRUNCATE":
        write_type = bigquery.WriteDisposition.WRITE_TRUNCATE
    else:
        raise ValueError("Invalid write type. Supported types are APPEND or WRITE_TRUNCATE")

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


def find_closest_matches(list1: list, list2: list):
    matches = {}
    for i in range(len(list1)):
        if list1[i] == list2[i]:
            matches[list1[i]] = list2[i]
    unmatched_list1 = [item for item in list1 if item not in matches]
    unmatched_list2 = [item for item in list2 if item not in matches.values()]
    threshold = 1.0

    while unmatched_list1:
        closest_match = {}
        for item1 in unmatched_list1:
            for item2 in unmatched_list2:
                similarity = difflib.SequenceMatcher(None, item1, item2).ratio()
                if similarity >= threshold and (item1 not in closest_match or similarity > closest_match[item1][0]):
                    closest_match[item1] = (similarity, item2)
        for item1, (_, item2) in closest_match.items():
            matches[item1] = item2
            unmatched_list1.remove(item1)
            unmatched_list2.remove(item2)

        threshold -= 0.1
    return matches


def possession_adjust(row: pd.Series, metric: str):
    opp_possession = 100 - row["Poss"]
    assert opp_possession >= 0, f"{row['Poss']} is not a valid possession value for metric {metric}"
    return row[metric] / opp_possession * 50


def find_most_similar_string(string: str, list_of_strings: list):
    matches = difflib.get_close_matches(string, list_of_strings)
    if len(matches) > 0:
        return matches[0]
    else:
        return None


def check_size(dataset_name: str):  # checks and prints out size of entire data set in MB
    client = bigquery.Client()
    # query = "SELECT sum(size_bytes) from Event_Data.__TABLES__"
    query = f"SELECT sum(size_bytes) FROM `{dataset_name}.__TABLES__`"
    job = client.query(query)
    for row in job:
        size = row[0] / (10**6)
    print("Current Data Size is", size, "MB")


# if current environment is ubuntu
def is_ubuntu():
    try:
        with open("/etc/os-release", "r") as file:
            content = file.read()
            if "Ubuntu" in content:
                return True
            else:
                return False
    except FileNotFoundError:
        return False


def get_system_usage():
    # Get RAM usage
    ram = psutil.virtual_memory()
    ram_total = ram.total / (1024**3)  # Convert bytes to GB
    ram_used = ram.used / (1024**3)  # Convert bytes to GB
    ram_free = ram.free / (1024**3)  # Convert bytes to GB

    # Get Disk usage
    disk = psutil.disk_usage("/")
    disk_total = disk.total / (1024**3)  # Convert bytes to GB
    disk_used = disk.used / (1024**3)  # Convert bytes to GB
    disk_free = disk.free / (1024**3)  # Convert bytes to GB

    return {"ram": {"total": ram_total, "used": ram_used, "free": ram_free}, "disk": {"total": disk_total, "used": disk_used, "free": disk_free}}
