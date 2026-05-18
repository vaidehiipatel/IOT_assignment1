import time
import json
import pandas as pd
import numpy as np
from confluent_kafka import Producer  # Updated import
from dotenv import load_dotenv
import os

load_dotenv()

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
API_KEY   = os.environ["KAFKA_API_KEY"]
API_SECRET = os.environ["KAFKA_API_SECRET"]

# Confluent Kafka uses a configuration dictionary
conf = {
    'bootstrap.servers': BOOTSTRAP,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': API_KEY,
    'sasl.password': API_SECRET
}

producer = Producer(conf)

# Optional callback function to confirm message receipt
def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Confirmed by broker: {msg.topic()} [{msg.partition()}]")

df = pd.read_csv("data/AirQualityUCI.csv", sep=";", decimal=",")
df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
df.dropna(how="all", inplace=True)
df.replace(-200, np.nan, inplace=True)

feature_cols = ["PT08.S1(CO)", "PT08.S2(NMHC)", "PT08.S3(NOx)",
                "PT08.S4(NO2)", "PT08.S5(O3)", "T", "RH", "AH"]

df_clean = df.dropna(subset=feature_cols).reset_index(drop=True)

print(f"Streaming {len(df_clean)} rows to 'raw-data' topic...")

for i, row in df_clean.iterrows():
    record = {col: float(row[col]) for col in feature_cols}
    record["date"] = str(row.get("Date", ""))
    record["time"] = str(row.get("Time", ""))
    record["row_index"] = int(i)

    # Trigger async transmission
    producer.produce(
        "raw-data", 
        value=json.dumps(record).encode("utf-8"), 
        callback=delivery_report
    )
    
    # Flush ensures network transmission completes instantly
    producer.flush() 
    print(f"[{i}] Sent: {record}")
    time.sleep(1)

print("Done.")
