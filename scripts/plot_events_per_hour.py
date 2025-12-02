import pandas as pd
import matplotlib.pyplot as plt

parquet_file = "gharchive_jan5_11.parquet"

df = pd.read_parquet(parquet_file, engine='pyarrow')

# Extract hour from timestamp
df['hour'] = pd.to_datetime(df['created_at']).dt.hour

# Count events per hour
events_per_hour = df.groupby('hour').size()

plt.figure(figsize=(10,6))
plt.plot(events_per_hour.index, events_per_hour.values, marker='o', color='red')
plt.title("Average GitHub Events per Hour – Jan 5-11, 2015")
plt.xlabel("Hour of Day")
plt.ylabel("Number of Events")
plt.xticks(range(24))
plt.grid(True, linestyle='--', alpha=0.5)
plt.tight_layout()
plt.show()
