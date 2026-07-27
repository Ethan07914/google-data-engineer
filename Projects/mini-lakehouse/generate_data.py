# generate_data.py
import csv
import random
from datetime import datetime, timedelta

random.seed(42)
start = datetime(2025, 1, 1)

with open("web_log.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["customer_id", "log_id", "timestamp", "url", "purchase_amount"])
    for i in range(5000):
        writer.writerow([
            random.randint(1, 500),
            i,
            (start + timedelta(minutes=random.randint(0, 200000))).isoformat(),
            random.choice(["/home", "/product/1", "/product/2", "/cart", "/checkout"]),
            round(random.uniform(0, 200), 2) if random.random() > 0.7 else 0,
        ])