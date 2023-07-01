import pandas as pd 
import os 
project_id = os.environ["GCP_PROJECT_NAME"]

cols = ["Standard_Player", 'Playing_Time_Playing_Time_Min',"Standard_Pos"]
col_str = ", ".join(cols)
items = ["MF"]
conditions = ' OR '.join([f"Standard_Pos LIKE '%{item}%'" for item in items])
query = f"""SELECT Standard_Player FROM `{project_id}.Stats.Players_2023` WHERE {conditions}"""
test_df = pd.read_gbq(query, project_id=project_id)
print(test_df.head())