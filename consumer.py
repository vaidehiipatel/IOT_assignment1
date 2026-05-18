import json
from kafka import KafkaConsumer
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

consumer = KafkaConsumer(
    "predictions",
    bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username=os.environ["KAFKA_API_KEY"],
    sasl_plain_password=os.environ["KAFKA_API_SECRET"],
    auto_offset_reset="latest",      # only new messages; use "earliest" to replay all
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    group_id="output-consumer-group",
)

print("Listening on 'predictions' topic...\n")

for msg in consumer:
    data = msg.value
    ts = datetime.now().strftime("%H:%M:%S")
    row = data.get("row_index", "?")
    pred = data.get("predicted_CO_mg_m3", "?")
    temp = data.get("T", "?")
    print(f"[{ts}] Row {row:>4} | Temp: {temp:>6.1f}°C | Predicted CO: {pred:>7.4f} mg/m³")