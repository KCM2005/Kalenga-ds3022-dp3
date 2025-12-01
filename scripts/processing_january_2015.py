import os
import requests

os.makedirs("gharchive-2015-01", exist_ok=True)
base_url = "https://data.gharchive.org"

for day in range(1, 32):
    for hour in range(0, 24):
        url = f"{base_url}/2015-01-{day:02d}-{hour}.json.gz"
        filename = f"gharchive-2015-01/{day:02d}-{hour}.json.gz"
        if not os.path.exists(filename):
            print(f"Downloading {filename}")
            r = requests.get(url, stream=True)
            with open(filename, "wb") as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)
        else:
            print(f"{filename} exists, skipping.")
