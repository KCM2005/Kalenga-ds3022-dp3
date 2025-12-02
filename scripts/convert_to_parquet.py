import os
import pandas as pd

input_folder = "gharchive-2015-01"
week_days = set(range(5, 12))  # Jan 5–11

all_data = []

for filename in os.listdir(input_folder):
    if filename.endswith(".json.gz"):
        day = int(filename.split('-')[0])
        if day not in week_days:
            continue
        file_path = os.path.join(input_folder, filename)
        df = pd.read_json(file_path, lines=True, compression='gzip')
        all_data.append(df)

df_week = pd.concat(all_data, ignore_index=True)

parquet_file = "gharchive_jan5_11.parquet"
df_week.to_parquet(parquet_file, engine='pyarrow', index=False)
