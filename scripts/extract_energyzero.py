import requests, json
from datetime import datetime, timedelta
import os

def fetch_energy_data():
    os.makedirs("data/raw", exist_ok=True)

    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=7)

    url = f"https://api.energyzero.nl/v1/energyprices?fromDate={start_date.date()}T00:00:00.000Z&tillDate={end_date.date()}T23:59:59.999Z&interval=4&usageType=1&inclBtw=false"
    
    response = requests.get(url)
    data = response.json()

    file_name = f"data/raw/energy_{end_date.strftime('%Y%m%d_%H%M%S')}.json"
    with open(file_name, "w") as f:
        json.dump(data, f)
    print(f"✅ Saved raw data: {file_name}")

if __name__ == "__main__":
    fetch_energy_data()
