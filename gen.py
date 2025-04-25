# generate_data.py
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
from multiprocessing import Pool, cpu_count

OUTPUT_DIR = "parquet_data"
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2025, 1, 1)

def generate_data_for_day(day_str):
    day = datetime.strptime(day_str, "%Y-%m-%d")
    print(f"Generating data for {day_str}...")

    timestamps = pd.date_range(start=day, end=day + timedelta(days=1) - timedelta(seconds=1), freq='S')
    data = {
        "isoTime": timestamps,  # Save as datetime64[ns]
        "randomNumber": np.random.randint(200, 501, size=len(timestamps))
    }
    df = pd.DataFrame(data)

    # Create partitioned folder structure: year=YYYY/month=MM/day=DD
    folder = os.path.join(
        OUTPUT_DIR,
        f"year={day.year}",
        f"month={day.month:02d}",
        f"day={day.day:02d}"
    )
    os.makedirs(folder, exist_ok=True)

    pq.write_table(pa.Table.from_pandas(df), os.path.join(folder, "data.parquet"))

def main():
    current_day = START_DATE
    all_days = []
    while current_day < END_DATE:
        all_days.append(current_day.strftime("%Y-%m-%d"))
        current_day += timedelta(days=1)

    print(f"Starting generation with {cpu_count()} workers...")

    with Pool(processes=cpu_count()) as pool:
        pool.map(generate_data_for_day, all_days)

if __name__ == "__main__":
    main()
