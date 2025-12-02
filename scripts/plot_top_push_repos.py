import pandas as pd
import matplotlib.pyplot as plt

parquet_file = "gharchive_jan5_11.parquet"

df = pd.read_parquet(parquet_file, engine='pyarrow')

# Filter PushEvents and count top repos
push_events = df[df['type'] == "PushEvent"]
top_repos = push_events['repo'].apply(lambda x: x['name']).value_counts().head(10)

plt.figure(figsize=(12,6))
plt.bar(top_repos.index, top_repos.values, color='blue')
plt.xticks(rotation=45, ha='right')
plt.xlabel("Repository")
plt.ylabel("Number of PushEvents")
plt.title("Top 10 Repositories by PushEvents – Jan 5–11, 2015")
plt.tight_layout()
plt.show()
